use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    FnArg, ItemFn, Pat, PatType, ReturnType, Type, TypeReference, parse_macro_input,
    spanned::Spanned,
};

/// Marks a function as a task. Supports zero or more parameters (including
/// references like `&i32`). Each parameter's type must be serde-serializable
/// after stripping one level of `&`.
///
/// The macro generates:
/// - A renamed inner function holding the user's body.
/// - A handler `fn(&str) -> Result<(), QueueError>` that deserializes the
///   tuple payload and invokes the inner function.
/// - A `pub static` named after the user's function, of type
///   `TaskDefinition<(T1, T2, ...)>`, whose `perform_async(args)` method
///   enqueues onto the globally-installed queue.
///
/// # Example
///
/// ```ignore
/// #[task]
/// fn sum(a: i32, b: i32) -> i32 { a + b }
///
/// sum.perform_async((1, 2))?;
/// ```
#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Default matches `Job::with_task_name` which sets `max_attempts = 3`.
    let mut max_attempts: u32 = 3;
    let mut priority_expr: TokenStream2 =
        quote! { ::job_queue_system::models::JobPriority::Normal };

    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("max_attempts") {
            let lit: syn::LitInt = meta.value()?.parse()?;
            max_attempts = lit.base10_parse()?;
            Ok(())
        } else if meta.path.is_ident("priority") {
            let expr: syn::Expr = meta.value()?.parse()?;
            priority_expr = quote! { #expr };
            Ok(())
        } else {
            Err(meta.error("unknown #[task] argument; expected `max_attempts` or `priority`"))
        }
    });
    parse_macro_input!(attr with parser);

    let input = parse_macro_input!(item as ItemFn);
    let vis = &input.vis;
    let fn_name = &input.sig.ident;
    let fn_name_str = fn_name.to_string();
    let inner_name = format_ident!("__{}_inner", fn_name);
    let handler_name = format_ident!("__{}_handler", fn_name);
    let task_struct_name = format_ident!("__{}_Task", fn_name);
    let block = &input.block;
    let inputs = &input.sig.inputs;
    let output = &input.sig.output;

    // Per-param: name, owned type (one `&` stripped), and call expression
    // forwarded to the inner fn (with `&` re-applied if the user took a ref).
    let mut param_names = Vec::new();
    let mut owned_tys = Vec::new();
    let mut call_exprs = Vec::new();

    for arg in inputs {
        let pt = match arg {
            FnArg::Typed(pt) => pt,
            FnArg::Receiver(r) => {
                return syn::Error::new(r.span(), "#[task] cannot be used on methods")
                    .to_compile_error()
                    .into();
            }
        };

        let ident = match extract_param_ident(pt) {
            Ok(i) => i,
            Err(e) => return e.to_compile_error().into(),
        };
        let (owned_ty, is_ref) = strip_one_ref(&pt.ty);
        param_names.push(ident.clone());
        owned_tys.push(owned_ty);
        call_exprs.push(if is_ref {
            quote! { &#ident }
        } else {
            quote! { #ident }
        });
    }

    let propagate_result = return_is_result(output);
    let call_inner = quote! { #inner_name(#(#call_exprs),*) };
    let invoke = if propagate_result {
        quote! { #call_inner }
    } else {
        quote! { { let _ = #call_inner; Ok(()) } }
    };

    // Payload is always a tuple so handler and perform_async agree on shape.
    let payload_tuple_ty = quote! { ( #(#owned_tys,)* ) };
    let payload_tuple_expr = quote! { ( #(#param_names,)* ) };
    let destructure_pat = quote! { ( #(#param_names,)* ) };

    let expanded = quote! {
        fn #inner_name(#inputs) #output #block

        fn #handler_name(__payload: &str) -> ::std::result::Result<(), ::job_queue_system::error::QueueError> {
            let #destructure_pat: #payload_tuple_ty =
                ::job_queue_system::task::deserialize_payload(#fn_name_str, __payload)?;
            #invoke
        }

        #[allow(non_camel_case_types)]
        #vis struct #task_struct_name {
            pub name: &'static str,
            pub handler: ::job_queue_system::task::TaskHandler,
        }

        impl #task_struct_name {
            /// Enqueue this task onto the globally-installed queue using the
            /// `max_attempts` and `priority` baked in at macro expansion.
            pub fn perform_async(&self, #(#param_names: #owned_tys),*)
                -> ::std::result::Result<(), ::job_queue_system::error::QueueError>
            {
                ::job_queue_system::task::enqueue_with_opts(
                    self.name,
                    #payload_tuple_expr,
                    #max_attempts,
                    #priority_expr,
                )
            }
        }

        #[allow(non_upper_case_globals)]
        #vis static #fn_name: #task_struct_name = #task_struct_name {
            name: #fn_name_str,
            handler: #handler_name,
        };
    };

    expanded.into()
}

fn extract_param_ident(pt: &PatType) -> syn::Result<syn::Ident> {
    if let Pat::Ident(pi) = &*pt.pat {
        Ok(pi.ident.clone())
    } else {
        Err(syn::Error::new(
            pt.pat.span(),
            "#[task] parameters must be simple identifiers",
        ))
    }
}

fn strip_one_ref(ty: &Type) -> (TokenStream2, bool) {
    if let Type::Reference(TypeReference { elem, .. }) = ty {
        (quote! { #elem }, true)
    } else {
        (quote! { #ty }, false)
    }
}

fn return_is_result(output: &ReturnType) -> bool {
    match output {
        ReturnType::Default => false,
        ReturnType::Type(_, ty) => {
            if let Type::Path(tp) = &**ty {
                tp.path
                    .segments
                    .last()
                    .map(|s| s.ident == "Result")
                    .unwrap_or(false)
            } else {
                false
            }
        }
    }
}

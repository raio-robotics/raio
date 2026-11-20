//! The macros of raio. Use them through `raio`, which exports them again.

use proc_macro::TokenStream;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{
    Data, DeriveInput, Error, Fields, FnArg, Ident, ImplItem, ImplItemFn, ItemImpl, Token,
    parse_macro_input,
};

/// Name the ports of a node after the fields that hold them; a diagram shows these names.
///
/// ```ignore
/// #[derive(Ports)]
/// struct Sensor {
///     node: Node,
///     reading: OutputPort<f64>,
/// }
/// ```
#[proc_macro_derive(Ports)]
pub fn derive_ports(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    // Only the fields of a struct have names to give.
    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => fields.named.iter().collect(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    };
    let fields = fields.iter().map(|field| &field.ident);
    quote! {
        impl #impl_generics ::raio::app::Ports for #name #type_generics #where_clause {
            fn names(&self) -> ::std::vec::Vec<(usize, &'static str)> {
                // A field that holds a port finds `PortField`, and any other field `OtherField`.
                #[allow(unused_imports)]
                use ::raio::app::{OtherField as _, PortField as _};
                #[allow(unused_mut)]
                let mut names = ::std::vec::Vec::new();
                #(
                    if let ::std::option::Option::Some(position) =
                        (&::raio::app::Field(&self.#fields)).position()
                    {
                        names.push((position, ::std::stringify!(#fields)));
                    }
                )*
                names
            }
        }
    }
    .into()
}

/// What a method of a `#[node]` block is declared as.
enum Declared {
    Task,
    /// The fields that hold the reaction's triggers.
    Reaction(Vec<Ident>),
    Shutdown,
}

/// Take the `#[task]`, `#[reaction(...)]` or `#[shutdown]` attribute off a method and return
/// what it declares; a method has at most one of them.
fn declaration(method: &mut ImplItemFn) -> syn::Result<Option<Declared>> {
    let mut declared = None;
    let mut kept = Vec::new();
    for attribute in method.attrs.drain(..) {
        let path = attribute.path();
        let found = if path.is_ident("task") {
            attribute.meta.require_path_only()?;
            Declared::Task
        } else if path.is_ident("reaction") {
            let triggers =
                attribute.parse_args_with(Punctuated::<Ident, Token![,]>::parse_terminated)?;
            if triggers.is_empty() {
                let message = "a reaction names the ports or actions that trigger it";
                return Err(Error::new(attribute.span(), message));
            }
            Declared::Reaction(triggers.into_iter().collect())
        } else if path.is_ident("shutdown") {
            attribute.meta.require_path_only()?;
            Declared::Shutdown
        } else {
            kept.push(attribute);
            continue;
        };
        if declared.is_some() {
            let message = "a method is a task, a reaction, or the shutdown function, not several";
            return Err(Error::new(attribute.span(), message));
        }
        declared = Some(found);
    }
    method.attrs = kept;
    Ok(declared)
}

/// The expression that calls a declared method on `node`, an `Rc` of the node. The method
/// takes `&self` or `self: Rc<Self>`, and nothing else.
fn call(method: &ImplItemFn) -> syn::Result<proc_macro2::TokenStream> {
    let name = &method.sig.ident;
    let signature = &method.sig;
    let Some(FnArg::Receiver(receiver)) = signature.inputs.first() else {
        let message = "a declared method takes `&self` or `self: Rc<Self>`";
        return Err(Error::new(signature.span(), message));
    };
    if signature.inputs.len() > 1 {
        let message = "a declared method takes no arguments; what it needs is in the node's fields";
        return Err(Error::new(signature.inputs.span(), message));
    }
    Ok(match receiver.colon_token {
        // `self: Rc<Self>` takes the node itself.
        Some(_) => quote!(Self::#name(::std::rc::Rc::clone(&node))),
        None => quote!(Self::#name(&node)),
    })
}

/// Declare the tasks, the reactions and the shutdown function of a node on its `impl` block.
///
/// A task is an `async` method that starts with the node. A reaction is a method that runs
/// once at each timestamp at which one of its triggers has a value, until all of them have
/// closed. The shutdown function runs once when the node shuts down. Tasks and reactions start
/// in the order in which the block declares them.
///
/// ```ignore
/// #[node]
/// impl Listener {
///     #[task]
///     async fn log(&self) {
///         self.node.wait(1 * S).await;
///     }
///
///     #[reaction(inp)]
///     fn show(&self) {
///         println!("{}", *self.inp.get().unwrap());
///     }
///
///     #[shutdown]
///     fn close(&self) {}
/// }
/// ```
#[proc_macro_attribute]
pub fn node(arguments: TokenStream, input: TokenStream) -> TokenStream {
    if !arguments.is_empty() {
        let message = "#[node] takes no arguments";
        return Error::new(proc_macro2::Span::call_site(), message)
            .to_compile_error()
            .into();
    }
    let mut block = parse_macro_input!(input as ItemImpl);
    let expanded = expand_node(&mut block).unwrap_or_else(|error| {
        // The block stays, with a node that runs nothing, so that the one error is reported.
        for item in &mut block.items {
            if let ImplItem::Fn(method) = item {
                let declares = |name: &str| ["task", "reaction", "shutdown"].contains(&name);
                method.attrs.retain(|attribute| {
                    !attribute
                        .path()
                        .get_ident()
                        .is_some_and(|name| declares(&name.to_string()))
                });
            }
        }
        let (impl_generics, _, where_clause) = block.generics.split_for_impl();
        let node = &block.self_ty;
        let error = error.into_compile_error();
        quote! {
            #error
            #block
            impl #impl_generics ::raio::app::Tasks for #node #where_clause {}
        }
    });
    expanded.into()
}

fn expand_node(block: &mut ItemImpl) -> syn::Result<proc_macro2::TokenStream> {
    if let Some((_, path, _)) = &block.trait_ {
        let message = "#[node] goes on the node's own `impl` block, not on the impl of a trait";
        return Err(Error::new(path.span(), message));
    }
    let mut starts = Vec::new();
    let mut shutdown = None;
    for item in &mut block.items {
        let ImplItem::Fn(method) = item else { continue };
        let Some(declared) = declaration(method)? else {
            continue;
        };
        let name = &method.sig.ident;
        let label = name.to_string();
        let asynchronous = method.sig.asyncness.is_some();
        let call = call(method)?;
        match declared {
            Declared::Task => {
                if !asynchronous {
                    return Err(Error::new(method.sig.span(), "a task is an `async` method"));
                }
                starts.push(quote! {
                    ::raio::app::Start::task(
                        #label,
                        |node: ::std::rc::Rc<Self>| ::std::boxed::Box::pin(async move {
                            #call.await
                        }),
                    )
                });
            }
            Declared::Reaction(triggers) => {
                let fields = triggers.iter().map(Ident::to_string);
                let wait = asynchronous.then(|| quote!(.await));
                starts.push(quote! {
                    ::raio::app::Start::reaction(
                        #label,
                        &[#(#fields),*],
                        |node: ::std::rc::Rc<Self>| ::std::boxed::Box::pin(async move {
                            // A closed trigger stays closed, so every round waits for all again.
                            while ::raio::app::next_event(::std::vec![
                                #(node.#triggers.trigger()),*
                            ])
                            .await
                            {
                                #call #wait;
                            }
                        }),
                    )
                });
            }
            Declared::Shutdown => {
                if asynchronous {
                    let message = "the shutdown function is not `async`: nothing runs after it";
                    return Err(Error::new(method.sig.span(), message));
                }
                if shutdown.is_some() {
                    let message = "a node has one shutdown function";
                    return Err(Error::new(method.sig.span(), message));
                }
                shutdown = Some(quote!(Self::#name(self)));
            }
        }
    }
    let (impl_generics, _, where_clause) = block.generics.split_for_impl();
    let node = &block.self_ty;
    Ok(quote! {
        #block

        impl #impl_generics ::raio::app::Tasks for #node #where_clause {
            fn starts(&self) -> ::std::vec::Vec<::raio::app::Start<Self>> {
                ::std::vec![#(#starts),*]
            }

            fn on_shutdown(&self) {
                #shutdown
            }
        }
    })
}

//! The derive macros of raio. Use them through `raio`, which exports them again.

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

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

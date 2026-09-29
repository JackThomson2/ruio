//! The `#[ruio::main]` and `#[ruio::test]` attributes. Use them through the
//! `ruio` crate, whose paths their expansions name.
//!
//! Both turn an `async fn` into a plain one whose body runs on a new ruio
//! runtime:
//!
//! ```text
//! #[ruio::main]
//! async fn main() -> io::Result<()> { body }
//! ```
//!
//! becomes
//!
//! ```text
//! fn main() -> io::Result<()> { ::ruio::__private::block_on(async move { body }) }
//! ```
//!
//! The tokens of the body are passed through untouched, so errors in it point
//! at the right place.

use proc_macro::{Delimiter, Group, Ident, Literal, Punct, Spacing, Span, TokenStream, TokenTree};

/// Runs an `async fn main` on a ruio runtime.
///
/// The function keeps its attributes, visibility and return type, loses its
/// `async`, and blocks on its body with a new [`ruio::Runtime`], panicking if
/// the runtime cannot start.
///
/// [`ruio::Runtime`]: https://docs.rs/ruio/latest/ruio/struct.Runtime.html
#[proc_macro_attribute]
pub fn main(args: TokenStream, item: TokenStream) -> TokenStream {
    expand("main", args, item, TokenStream::new())
}

/// Runs an `async fn` test on a ruio runtime of its own.
///
/// Like [`macro@main`], and adds `#[test]`; other attributes, such as
/// `#[should_panic]`, still apply.
#[proc_macro_attribute]
pub fn test(args: TokenStream, item: TokenStream) -> TokenStream {
    let test = "#[::core::prelude::v1::test]"
        .parse()
        .expect("valid tokens");
    expand("test", args, item, test)
}

fn expand(name: &str, args: TokenStream, item: TokenStream, prefix: TokenStream) -> TokenStream {
    // Arguments are reported, and the function still expanded, so the
    // error stands alone.
    let mut output = match args.into_iter().next() {
        Some(arg) => error(arg.span(), &format!("`#[ruio::{name}]` takes no arguments")),
        None => TokenStream::new(),
    };
    let mut tokens: Vec<TokenTree> = item.clone().into_iter().collect();
    // What cannot be expanded is reported and passed through as it is, so
    // that no follow-on errors bury the report.
    let reject = |span: Span, message: String| {
        let mut rejected = error(span, &message);
        rejected.extend(item.clone());
        rejected
    };
    let first = tokens.first().map_or_else(Span::call_site, TokenTree::span);
    let body = match tokens.pop() {
        Some(TokenTree::Group(body)) if body.delimiter() == Delimiter::Brace => body,
        _ => return reject(first, format!("`#[ruio::{name}]` goes on a function")),
    };
    let Some(fn_position) = tokens.iter().position(|token| is_ident(token, "fn")) else {
        return reject(first, format!("`#[ruio::{name}]` goes on a function"));
    };
    // Attributes are groups, so an `async` before `fn` is a qualifier.
    let Some(async_position) = tokens[..fn_position]
        .iter()
        .position(|token| is_ident(token, "async"))
    else {
        let span = tokens[fn_position].span();
        return reject(span, format!("`#[ruio::{name}]` needs an `async fn`"));
    };
    tokens.remove(async_position);

    let mut block_on: TokenStream = "::ruio::__private::block_on".parse().expect("valid tokens");
    let future = TokenStream::from_iter([
        TokenTree::Ident(Ident::new("async", body.span())),
        TokenTree::Ident(Ident::new("move", body.span())),
        TokenTree::Group(body.clone()),
    ]);
    block_on.extend([TokenTree::Group(Group::new(Delimiter::Parenthesis, future))]);
    let mut new_body = Group::new(Delimiter::Brace, block_on);
    new_body.set_span(body.span());

    output.extend(prefix);
    output.extend(tokens);
    output.extend([TokenTree::Group(new_body)]);
    output
}

fn is_ident(token: &TokenTree, name: &str) -> bool {
    matches!(token, TokenTree::Ident(ident) if ident.to_string() == name)
}

/// `::core::compile_error!(message)`, pointing at `span`.
fn error(span: Span, message: &str) -> TokenStream {
    let mut literal = Literal::string(message);
    literal.set_span(span);
    let mut arguments = Group::new(
        Delimiter::Parenthesis,
        TokenStream::from_iter([TokenTree::Literal(literal)]),
    );
    arguments.set_span(span);
    let tokens = [
        TokenTree::Punct(Punct::new(':', Spacing::Joint)),
        TokenTree::Punct(Punct::new(':', Spacing::Alone)),
        TokenTree::Ident(Ident::new("core", span)),
        TokenTree::Punct(Punct::new(':', Spacing::Joint)),
        TokenTree::Punct(Punct::new(':', Spacing::Alone)),
        TokenTree::Ident(Ident::new("compile_error", span)),
        TokenTree::Punct(Punct::new('!', Spacing::Alone)),
        TokenTree::Group(arguments),
        TokenTree::Punct(Punct::new(';', Spacing::Alone)),
    ];
    tokens
        .into_iter()
        .map(|mut token| {
            token.set_span(span);
            token
        })
        .collect()
}

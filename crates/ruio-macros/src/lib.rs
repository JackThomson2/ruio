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
//! With `cores = "all"` or `cores = N`, the body runs on a runtime on each of
//! several cores instead, each core making a future of its own:
//!
//! ```text
//! fn main() -> io::Result<()> {
//!     ::ruio::__private::block_on_cores(None, |_: ::ruio::Core| async move { body })
//! }
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
/// `#[ruio::main(cores = "all")]` runs the body on every CPU the process may
/// run on instead, each on a runtime of its own, and `cores = N` on the
/// first `N`; see [`ruio::Cores::run`]. The function returns once every core
/// has finished: `()`, or a `Result<(), E>` with the first core's error, in
/// core order. As every core runs the body, it can use the function's
/// arguments only by reference or by copy.
///
/// [`ruio::Runtime`]: https://docs.rs/ruio/latest/ruio/struct.Runtime.html
/// [`ruio::Cores::run`]: https://docs.rs/ruio/latest/ruio/struct.Cores.html#method.run
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
    // Bad arguments are reported, and the function still expanded, onto one
    // runtime, so the error stands alone.
    let (cores, mut output) = match cores(name, args) {
        Ok(cores) => (cores, TokenStream::new()),
        Err(error) => (None, error),
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

    let future = TokenStream::from_iter([
        TokenTree::Ident(Ident::new("async", body.span())),
        TokenTree::Ident(Ident::new("move", body.span())),
        TokenTree::Group(body.clone()),
    ]);
    let (run, arguments) = match cores {
        None => ("::ruio::__private::block_on", future),
        Some(mut arguments) => {
            arguments.extend([TokenTree::Punct(Punct::new(',', Spacing::Alone))]);
            // Every core runs the body, in a future of its own.
            arguments.extend(
                "|_: ::ruio::Core|"
                    .parse::<TokenStream>()
                    .expect("valid tokens"),
            );
            arguments.extend(future);
            ("::ruio::__private::block_on_cores", arguments)
        }
    };
    let mut call: TokenStream = run.parse().expect("valid tokens");
    call.extend([TokenTree::Group(Group::new(
        Delimiter::Parenthesis,
        arguments,
    ))]);
    let mut new_body = Group::new(Delimiter::Brace, call);
    new_body.set_span(body.span());

    output.extend(prefix);
    output.extend(tokens);
    output.extend([TokenTree::Group(new_body)]);
    output
}

/// The cores to run on, from the attribute's arguments: `None` for one
/// runtime on the thread, or the count that `block_on_cores` takes, which is
/// `None` for every CPU.
fn cores(name: &str, args: TokenStream) -> Result<Option<TokenStream>, TokenStream> {
    let args: Vec<TokenTree> = args.into_iter().collect();
    let usage =
        format!("`#[ruio::{name}]` takes no arguments, or `cores = \"all\"` or `cores = N`");
    let [
        TokenTree::Ident(key),
        TokenTree::Punct(eq),
        TokenTree::Literal(value),
    ] = args.as_slice()
    else {
        return match args.first() {
            None => Ok(None),
            Some(arg) => Err(error(arg.span(), &usage)),
        };
    };
    if key.to_string() != "cores" || eq.as_char() != '=' {
        return Err(error(key.span(), &usage));
    }
    let count = match value.to_string().as_str() {
        "\"all\"" => "::core::option::Option::None"
            .parse()
            .expect("valid tokens"),
        count => match count.parse::<usize>() {
            Ok(0) => return Err(error(value.span(), "`cores` must be at least 1")),
            Ok(_) => {
                let mut some: TokenStream = "::core::option::Option::Some"
                    .parse()
                    .expect("valid tokens");
                let count = TokenStream::from(TokenTree::Literal(value.clone()));
                some.extend([TokenTree::Group(Group::new(Delimiter::Parenthesis, count))]);
                some
            }
            Err(_) => return Err(error(value.span(), &usage)),
        },
    };
    Ok(Some(count))
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

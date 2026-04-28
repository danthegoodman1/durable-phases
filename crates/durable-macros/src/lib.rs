use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::braced;
use syn::parenthesized;
use syn::parse::{Parse, ParseStream};
use syn::{
    parse_macro_input, Block, Expr, Ident, LitInt, LitStr, Path, Result, Token, Type, Visibility,
};

mod kw {
    syn::custom_keyword!(workflow);
    syn::custom_keyword!(name);
    syn::custom_keyword!(version);
    syn::custom_keyword!(input);
    syn::custom_keyword!(output);
    syn::custom_keyword!(common);
    syn::custom_keyword!(initial);
    syn::custom_keyword!(global);
    syn::custom_keyword!(queries);
    syn::custom_keyword!(migrations);
    syn::custom_keyword!(phase);
    syn::custom_keyword!(run);
    syn::custom_keyword!(on);
    syn::custom_keyword!(signal);
    syn::custom_keyword!(timer);
    syn::custom_keyword!(child);
    syn::custom_keyword!(query);
}

#[proc_macro]
pub fn workflow(input: TokenStream) -> TokenStream {
    let workflow = parse_macro_input!(input as WorkflowDef);
    expand_workflow(workflow)
        .unwrap_or_else(|error| error.to_compile_error())
        .into()
}

struct WorkflowDef {
    vis: Visibility,
    ident: Ident,
    name: LitStr,
    version: LitInt,
    input: Type,
    output: Type,
    common: Option<Type>,
    initial: InitialDef,
    global: Vec<WaitDef>,
    queries: Vec<QueryDef>,
    migrations: Vec<MigrationDef>,
    phases: Vec<PhaseDef>,
}

struct InitialDef {
    arg: Ident,
    block: Block,
}

struct PhaseDef {
    name: Ident,
    data_ident: Ident,
    data_ty: Type,
    mode: PhaseMode,
}

enum PhaseMode {
    Run(HandlerDef),
    On(Vec<WaitDef>),
}

struct WaitDef {
    name: Ident,
    kind: WaitKind,
    handler: HandlerDef,
}

enum WaitKind {
    Signal(Type),
    Timer(Expr),
    Child(Expr),
}

struct HandlerDef {
    _args: Vec<Ident>,
    block: Block,
}

struct QueryDef {
    name: Ident,
    output_ty: Type,
    _args: Vec<Ident>,
    block: Block,
}

struct MigrationDef {
    from_version: LitInt,
    function: Path,
}

impl Parse for WorkflowDef {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let vis: Visibility = input.parse()?;
        input.parse::<kw::workflow>()?;
        let ident: Ident = input.parse()?;

        let content;
        braced!(content in input);

        let mut name = None;
        let mut version = None;
        let mut input_ty = None;
        let mut output = None;
        let mut common = None;
        let mut initial = None;
        let mut global = Vec::new();
        let mut queries = Vec::new();
        let mut migrations = Vec::new();
        let mut phases = Vec::new();

        while !content.is_empty() {
            if content.peek(kw::name) {
                content.parse::<kw::name>()?;
                content.parse::<Token![:]>()?;
                name = Some(content.parse::<LitStr>()?);
                parse_optional_comma(&content)?;
            } else if content.peek(kw::version) {
                content.parse::<kw::version>()?;
                content.parse::<Token![:]>()?;
                version = Some(content.parse::<LitInt>()?);
                parse_optional_comma(&content)?;
            } else if content.peek(kw::input) {
                content.parse::<kw::input>()?;
                content.parse::<Token![:]>()?;
                input_ty = Some(content.parse::<Type>()?);
                parse_optional_comma(&content)?;
            } else if content.peek(kw::output) {
                content.parse::<kw::output>()?;
                content.parse::<Token![:]>()?;
                output = Some(content.parse::<Type>()?);
                parse_optional_comma(&content)?;
            } else if content.peek(kw::common) {
                content.parse::<kw::common>()?;
                content.parse::<Token![:]>()?;
                common = Some(content.parse::<Type>()?);
                parse_optional_comma(&content)?;
            } else if content.peek(kw::initial) {
                initial = Some(parse_initial(&content)?);
            } else if content.peek(kw::global) {
                content.parse::<kw::global>()?;
                let body;
                braced!(body in content);
                global = parse_waits(&body)?;
            } else if content.peek(kw::queries) {
                content.parse::<kw::queries>()?;
                let body;
                braced!(body in content);
                queries = parse_queries(&body)?;
            } else if content.peek(kw::migrations) {
                content.parse::<kw::migrations>()?;
                let body;
                braced!(body in content);
                migrations = parse_migrations(&body)?;
            } else if content.peek(kw::phase) {
                phases.push(parse_phase(&content)?);
            } else {
                return Err(content.error(
                    "expected workflow field, initial, global, queries, migrations, or phase",
                ));
            }
        }

        Ok(Self {
            vis,
            ident,
            name: required(name, "workflow name")?,
            version: required(version, "workflow version")?,
            input: required(input_ty, "workflow input type")?,
            output: required(output, "workflow output type")?,
            common,
            initial: required(initial, "initial block")?,
            global,
            queries,
            migrations,
            phases,
        })
    }
}

fn required<T>(value: Option<T>, label: &str) -> Result<T> {
    value.ok_or_else(|| syn::Error::new(Span::call_site(), format!("missing {label}")))
}

fn parse_optional_comma(input: ParseStream<'_>) -> Result<()> {
    if input.peek(Token![,]) {
        input.parse::<Token![,]>()?;
    }
    Ok(())
}

fn parse_initial(input: ParseStream<'_>) -> Result<InitialDef> {
    input.parse::<kw::initial>()?;
    let args;
    parenthesized!(args in input);
    let arg = args.parse::<Ident>()?;
    let block = input.parse::<Block>()?;
    Ok(InitialDef { arg, block })
}

fn parse_phase(input: ParseStream<'_>) -> Result<PhaseDef> {
    input.parse::<kw::phase>()?;
    let name = input.parse::<Ident>()?;
    let args;
    parenthesized!(args in input);
    let data_ident = args.parse::<Ident>()?;
    args.parse::<Token![:]>()?;
    let data_ty = args.parse::<Type>()?;

    let body;
    braced!(body in input);

    let mode = if body.peek(kw::run) {
        body.parse::<kw::run>()?;
        PhaseMode::Run(parse_async_handler(&body)?)
    } else if body.peek(kw::on) {
        body.parse::<kw::on>()?;
        let waits_body;
        braced!(waits_body in body);
        PhaseMode::On(parse_waits(&waits_body)?)
    } else {
        return Err(body.error("phase must contain run or on"));
    };

    Ok(PhaseDef {
        name,
        data_ident,
        data_ty,
        mode,
    })
}

fn parse_waits(input: ParseStream<'_>) -> Result<Vec<WaitDef>> {
    let mut waits = Vec::new();
    while !input.is_empty() {
        let name = input.parse::<Ident>()?;
        input.parse::<Token![:]>()?;

        let kind = if input.peek(kw::signal) {
            input.parse::<kw::signal>()?;
            input.parse::<Token![<]>()?;
            let event_ty = input.parse::<Type>()?;
            input.parse::<Token![>]>()?;
            WaitKind::Signal(event_ty)
        } else if input.peek(kw::timer) {
            input.parse::<kw::timer>()?;
            let args;
            parenthesized!(args in input);
            WaitKind::Timer(args.parse::<Expr>()?)
        } else if input.peek(kw::child) {
            input.parse::<kw::child>()?;
            let args;
            parenthesized!(args in input);
            WaitKind::Child(args.parse::<Expr>()?)
        } else {
            return Err(input.error("expected signal<T>, timer(expr), or child(expr)"));
        };

        let handler = parse_async_handler(input)?;
        waits.push(WaitDef {
            name,
            kind,
            handler,
        });
        parse_optional_comma(input)?;
    }
    Ok(waits)
}

fn parse_async_handler(input: ParseStream<'_>) -> Result<HandlerDef> {
    input.parse::<Token![async]>()?;
    let args = parse_pipe_args(input)?;
    let block = input.parse::<Block>()?;
    Ok(HandlerDef { _args: args, block })
}

fn parse_pipe_args(input: ParseStream<'_>) -> Result<Vec<Ident>> {
    input.parse::<Token![|]>()?;
    let mut args = Vec::new();
    while !input.peek(Token![|]) {
        args.push(input.parse::<Ident>()?);
        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
        } else {
            break;
        }
    }
    input.parse::<Token![|]>()?;
    Ok(args)
}

fn parse_queries(input: ParseStream<'_>) -> Result<Vec<QueryDef>> {
    let mut queries = Vec::new();
    while !input.is_empty() {
        let name = input.parse::<Ident>()?;
        input.parse::<Token![:]>()?;
        input.parse::<kw::query>()?;
        input.parse::<Token![<]>()?;
        let output_ty = input.parse::<Type>()?;
        input.parse::<Token![>]>()?;
        let args = parse_pipe_args(input)?;
        let block = input.parse::<Block>()?;
        queries.push(QueryDef {
            name,
            output_ty,
            _args: args,
            block,
        });
        parse_optional_comma(input)?;
    }
    Ok(queries)
}

fn parse_migrations(input: ParseStream<'_>) -> Result<Vec<MigrationDef>> {
    let mut migrations = Vec::new();
    while !input.is_empty() {
        let from_version = input.parse::<LitInt>()?;
        input.parse::<Token![:]>()?;
        let function = input.parse::<Path>()?;
        migrations.push(MigrationDef {
            from_version,
            function,
        });
        parse_optional_comma(input)?;
    }
    Ok(migrations)
}

fn expand_workflow(workflow: WorkflowDef) -> Result<TokenStream2> {
    let vis = workflow.vis;
    let inner_workflow_vis = match &vis {
        Visibility::Inherited => quote! { pub(super) },
        _ => quote! { #vis },
    };
    let workflow_ident = workflow.ident;
    let workflow_name = workflow.name;
    let workflow_version = workflow.version;
    let input_ty = workflow.input;
    let output_ty = workflow.output;
    let common_ty = workflow.common.unwrap_or_else(|| syn::parse_quote! { () });
    let initial_arg = workflow.initial.arg;
    let initial_block = workflow.initial.block;

    let module_ident = format_ident!(
        "__durable_workflow_{}",
        workflow_ident.to_string().to_lowercase()
    );
    let phase_enum_ident = format_ident!("{}Phase", workflow_ident);

    let mut variant_defs = Vec::new();
    let mut helper_defs = Vec::new();
    let mut phase_name_arms = Vec::new();
    let mut into_snapshot_arms = Vec::new();
    let mut from_snapshot_arms = Vec::new();
    let mut phase_action_arms = Vec::new();
    let mut dispatch_run_arms = Vec::new();
    let mut dispatch_event_arms = Vec::new();

    for phase in &workflow.phases {
        let phase_name = phase.name.to_string();
        let phase_name_lit = LitStr::new(&phase_name, phase.name.span());
        let helper_ident = &phase.name;
        let variant_ident = phase_variant_ident(&phase.name);
        let data_ident = &phase.data_ident;
        let data_ty = &phase.data_ty;

        variant_defs.push(quote! {
            #variant_ident(#data_ty)
        });

        helper_defs.push(quote! {
            fn #helper_ident(data: #data_ty) -> #phase_enum_ident {
                #phase_enum_ident::#variant_ident(data)
            }
        });

        phase_name_arms.push(quote! {
            #phase_enum_ident::#variant_ident(_) => #phase_name_lit
        });

        into_snapshot_arms.push(quote! {
            #phase_enum_ident::#variant_ident(data) => {
                Ok(::durable::PhaseSnapshot {
                    name: #phase_name_lit.to_string(),
                    data: ::serde_json::to_value(data)?,
                })
            }
        });

        from_snapshot_arms.push(quote! {
            #phase_name_lit => {
                Ok(#phase_enum_ident::#variant_ident(::serde_json::from_value(snapshot.data)?))
            }
        });

        match &phase.mode {
            PhaseMode::Run(handler) => {
                let block = &handler.block;
                phase_action_arms.push(quote! {
                    #phase_enum_ident::#variant_ident(_) => ::durable::PhaseAction::run()
                });
                dispatch_run_arms.push(quote! {
                    #phase_enum_ident::#variant_ident(#data_ident) => {
                        let _ = &ctx;
                        let _ = &common;
                        let _ = &#data_ident;
                        #block
                    }
                });
            }
            PhaseMode::On(waits) => {
                let wait_specs = waits.iter().map(expand_wait_spec);
                phase_action_arms.push(quote! {
                    #phase_enum_ident::#variant_ident(#data_ident) => {
                        let _ = #data_ident;
                        let mut waits = Vec::new();
                        #(#wait_specs)*
                        ::durable::PhaseAction::wait(waits)
                    }
                });

                let event_wait_arms = waits.iter().map(|wait| {
                    expand_phase_event_arm(&phase_enum_ident, &variant_ident, data_ident, wait)
                });
                dispatch_event_arms.extend(event_wait_arms);
            }
        }
    }

    let global_wait_specs = workflow.global.iter().map(|wait| {
        let wait_name_lit = LitStr::new(&wait.name.to_string(), wait.name.span());
        match &wait.kind {
            WaitKind::Signal(event_ty) => quote! {
                waits.push(::durable::WaitSpec::signal::<#event_ty>(#wait_name_lit));
            },
            WaitKind::Timer(_) | WaitKind::Child(_) => {
                syn::Error::new(wait.name.span(), "global waits only support signals")
                    .to_compile_error()
            }
        }
    });

    let global_event_arms = workflow.global.iter().map(expand_global_event_arm);
    dispatch_event_arms.extend(global_event_arms);

    let query_arms = workflow.queries.iter().map(|query| {
        let query_name_lit = LitStr::new(&query.name.to_string(), query.name.span());
        let output_ty = &query.output_ty;
        let block = &query.block;
        quote! {
            #query_name_lit => {
                let output: #output_ty = #block;
                Ok(::serde_json::to_value(output)?)
            }
        }
    });

    let migration_arms = workflow.migrations.iter().map(|migration| {
        let from_version = &migration.from_version;
        let function = &migration.function;
        quote! {
            #from_version => {
                let result = ::durable::IntoMigrationOutput::<Self::Common, Self::Phase>::into_output(#function(args))?;
                Ok(Some(result))
            }
        }
    });

    Ok(quote! {
        mod #module_ident {
            use super::*;

            #[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize)]
            pub enum #phase_enum_ident {
                #(#variant_defs,)*
            }

            #(#helper_defs)*

            #inner_workflow_vis struct #workflow_ident;

            impl ::durable::DurablePhase for #phase_enum_ident {
                fn phase_name(&self) -> &'static str {
                    match self {
                        #(#phase_name_arms,)*
                    }
                }

                fn into_snapshot(self) -> Result<::durable::PhaseSnapshot, ::durable::WorkflowError> {
                    match self {
                        #(#into_snapshot_arms,)*
                    }
                }

                fn from_snapshot(snapshot: ::durable::PhaseSnapshot) -> Result<Self, ::durable::WorkflowError> {
                    match snapshot.name.as_str() {
                        #(#from_snapshot_arms,)*
                        other => Err(::durable::WorkflowError::new(format!("unknown phase: {other}"))),
                    }
                }
            }

            #[::durable::async_trait]
            impl ::durable::Workflow for #workflow_ident {
                type Input = #input_ty;
                type Output = #output_ty;
                type Common = #common_ty;
                type Phase = #phase_enum_ident;

                const NAME: &'static str = #workflow_name;
                const VERSION: u32 = #workflow_version;

                fn initial(#initial_arg: Self::Input) -> ::durable::Start<Self::Common, Self::Phase> {
                    #initial_block
                }

                fn global_waits() -> Vec<::durable::WaitSpec> {
                    let mut waits = Vec::new();
                    #(#global_wait_specs)*
                    waits
                }

                fn phase_action(phase: &Self::Phase) -> ::durable::PhaseAction {
                    match phase {
                        #(#phase_action_arms,)*
                    }
                }

                async fn dispatch_run(
                    ctx: &mut ::durable::DurableContext,
                    common: Self::Common,
                    phase: Self::Phase,
                ) -> Result<::durable::Transition<Self::Output, Self::Phase>, ::durable::WorkflowError> {
                    match phase {
                        #(#dispatch_run_arms,)*
                        _ => Err(::durable::WorkflowError::new("phase is not runnable")),
                    }
                }

                async fn dispatch_event(
                    ctx: &mut ::durable::DurableContext,
                    common: Self::Common,
                    phase: Self::Phase,
                    wait_name: &str,
                    event: ::durable::ReadyEvent,
                ) -> Result<::durable::Transition<Self::Output, Self::Phase>, ::durable::WorkflowError> {
                    match wait_name {
                        #(#dispatch_event_arms,)*
                        other => Err(::durable::WorkflowError::new(format!("unknown wait: {other}"))),
                    }
                }

                fn query(
                    name: &str,
                    snapshot: ::durable::InstanceSnapshot<Self::Output, Self::Common, Self::Phase>,
                    sequence: u64,
                ) -> Result<::serde_json::Value, ::durable::WorkflowError> {
                    match name {
                        #(#query_arms,)*
                        other => Err(::durable::WorkflowError::new(format!("unknown query: {other}"))),
                    }
                }

                fn migrate(
                    from_version: u32,
                    args: ::durable::MigrationArgs,
                ) -> Result<Option<::durable::MigrationResult<Self::Common, Self::Phase>>, ::durable::WorkflowError> {
                    match from_version {
                        #(#migration_arms,)*
                        _ => Ok(None),
                    }
                }
            }
        }

        #[allow(unused_imports)]
        #vis use self::#module_ident::{#workflow_ident, #phase_enum_ident};
    })
}

fn phase_variant_ident(phase_name: &Ident) -> Ident {
    let name = phase_name.to_string();
    let mut output = String::new();
    let mut uppercase_next = true;
    for character in name.chars() {
        if character == '_' {
            uppercase_next = true;
            continue;
        }
        if uppercase_next {
            output.extend(character.to_uppercase());
            uppercase_next = false;
        } else {
            output.push(character);
        }
    }
    Ident::new(&output, phase_name.span())
}

fn expand_wait_spec(wait: &WaitDef) -> TokenStream2 {
    let wait_name_lit = LitStr::new(&wait.name.to_string(), wait.name.span());
    match &wait.kind {
        WaitKind::Signal(event_ty) => quote! {
            waits.push(::durable::WaitSpec::signal::<#event_ty>(#wait_name_lit));
        },
        WaitKind::Timer(expr) => quote! {
            if let Some(fire_at) = ::durable::IntoTimerFireAt::into_fire_at(#expr) {
                waits.push(::durable::WaitSpec::timer(#wait_name_lit, fire_at));
            }
        },
        WaitKind::Child(expr) => quote! {
            if let Some(wait) = ::durable::IntoChildWait::into_wait_spec(#expr, #wait_name_lit) {
                waits.push(wait);
            }
        },
    }
}

fn expand_phase_event_arm(
    phase_enum_ident: &Ident,
    variant_ident: &Ident,
    data_ident: &Ident,
    wait: &WaitDef,
) -> TokenStream2 {
    let wait_name_lit = LitStr::new(&wait.name.to_string(), wait.name.span());
    let block = &wait.handler.block;
    let decode_event = match &wait.kind {
        WaitKind::Signal(event_ty) => quote! {
            let event: #event_ty = ::durable::decode_signal_event(event)?;
        },
        WaitKind::Timer(_) => quote! {
            let event = ::durable::decode_timer_event(event)?;
        },
        WaitKind::Child(expr) => quote! {
            let __handle = #expr;
            let event = ::durable::decode_child_event(&__handle, event)?;
        },
    };

    quote! {
        #wait_name_lit => {
            match phase {
                #phase_enum_ident::#variant_ident(#data_ident) => {
                    #decode_event
                    let _ = &ctx;
                    let _ = &common;
                    let _ = &#data_ident;
                    let _ = &event;
                    #block
                }
                _ => Err(::durable::WorkflowError::new(format!("wait {} is not active for current phase", #wait_name_lit))),
            }
        }
    }
}

fn expand_global_event_arm(wait: &WaitDef) -> TokenStream2 {
    let wait_name_lit = LitStr::new(&wait.name.to_string(), wait.name.span());
    let block = &wait.handler.block;
    match &wait.kind {
        WaitKind::Signal(event_ty) => quote! {
            #wait_name_lit => {
                let event: #event_ty = ::durable::decode_signal_event(event)?;
                let _ = &ctx;
                let _ = &common;
                let _ = &phase;
                let _ = &event;
                #block
            }
        },
        WaitKind::Timer(_) | WaitKind::Child(_) => {
            syn::Error::new(wait.name.span(), "global waits only support signals")
                .to_compile_error()
        }
    }
}

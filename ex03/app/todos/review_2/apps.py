
#
# do not import anything
#


def invoke_llm(llm, prompt):
    response = llm.invoke(prompt)
    return response

def get_calc_template(Templ):
    return Templ(
        input_variables=["expr"],
        template=(
            "You are a strict calculator. You only evaluate mathematical expressions like addition, "
            "subtraction, multiplication, division, square roots, and exponentiation. "
            "If the input is a valid mathematical expression, return it followed by its result. "
            "If the input is not a mathematical expression eg. 'hyp 3,4', return 'not an expression'.\n\n"
            "Input: {expr}\n"
            "Output:"
        )
    )


def invoke_llm_tmpl(llm, tmpl, expr):
    llm_chain = tmpl | llm
    return llm_chain.invoke({"expr": expr})


def get_chain(Chain, llm, tmpl ):
    chain = Chain(llm=llm, prompt=tmpl)
    return chain


def invoke_chain(chain, params):
    return chain.run(params)


def get_calc_template_ctx(Templ):
    return Templ(
        input_variables=["expr", "functions"],
        template=(
            "Only output the expression AND result of the expression. "
            "If the expression contains a custom function like 'hyp', evaluate it accordingly. "
            "If the expression is invalid, return 'not an expression'.\n\n"
            "Input: {expr}\n"
            "Output:"
        )
    )


def get_calc_context():
    context = {
        "functions": [
            {
                "name": "hyp",
                "description": "Calculate the length of the hypotenuse in a right-angled triangle given the lengths of the two legs.",
                "parameters": ["a", "b"],
                "logic": "sqrt(a^2 + b^2)",
                "example": {
                    "parameters": [3, 4],
                    "return_value": 5
                }
            }
        ]
    }
    return context


def get_chat_template(Templ):
    return Templ(
        input_variables=["chat_history", "question"],
        template=(
            "You are a helpful AI assistant. Keep your answers brief and to the point.\n"
            "Chat history: {chat_history}\n"
            "User's question: {question}\n"
            "Answer:"
        )
    )


def get_chat_memory(ChatMemory):
    return ChatMemory(memory_key="chat_history")


def get_chain_mem(Chain, llm, tmpl, chat_memory):
    chain = Chain(
        llm=llm,
        prompt=tmpl,
        memory=chat_memory
    )
    return chain

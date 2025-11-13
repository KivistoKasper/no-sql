
#
# do not import anything
#

# app 1
def invoke_llm(llm, prompt):
    return llm.invoke(prompt)

#app 2
def get_calc_template(Templ):
    return Templ(template="""
                 You are a calculator and your job is to calculate the given mathematical expression.
                 Interpret only mathematical expressions and limit response to the expression 
                 entered by the user, and its result. 
                 These are you commands: 
                 1) addition and subtraction: if the user inputs: 1 + 1, you answer with: 1 + 1 = 2
                 2) square root: if the user inputs: sqrt 9, you answer with: sqrt 9 = 3
                 3) if the user inputs something else, you answer with: not an expression
                Calculate this: {expr}
                """, input_variables=["expr"])


def invoke_llm_tmpl(llm, tmpl, expr):
    return llm.invoke(tmpl.format(expr=expr))

# app 3 
def get_chain(Chain, llm, tmpl ):
    return Chain(llm=llm, prompt=tmpl)

# check these again
def invoke_chain(chain, params):
    return chain(params)["text"]

# app 4
def get_calc_template_ctx(Templ):
    return Templ(template="""
                 You are a calculator and your job is to calculate the given mathematical expression.
                 Interpret only mathematical expressions and limit response to the expression 
                 entered by the user, and its result. 
                 These are you commands: 
                 1) addition and subtraction: if the user inputs: 1 + 1, you answer with: 1 + 1 = 2
                 2) square root: if the user inputs: sqrt 9, you answer with: sqrt 9 = 3
                 3) if the user inputs something else, you answer with: not an expression
                 additional functions: 
                 {functions}
                Calculate this: {expr}
                """, input_variables=["expr", "functions"])


def get_calc_context():
    return {
        "functions": [
            {
                "name": "hyp",
                "description": "hypotenuse of a right-angled triangle",
                "parameters": ["a", "b"],
                "logic": "sqrt(a^2 + b^2)",
                "example": {
                    "parameters": [3, 2], 
                    "return_value": "sqrt(3^2 + 2^2) = 3.605551275"
                    }
            }
        ]
    }

# app 5
def get_chat_template(Templ):
    template = "This is a conversation:\n{chat_history}\nUser: {question}\nAssistant:"
    return Templ.from_template(template)

# app 6
def get_chat_memory(ChatMemory):
    chat_history = ChatMemory(return_messages=False)
    return chat_history

def get_chain_mem(Chain, llm, tmpl, chat_memory):
    return Chain(llm=llm, prompt=tmpl, memory=chat_memory)


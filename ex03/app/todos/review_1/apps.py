
#
# do not import anything
#

# 2u3LAfEKDbggisPGOVrumOPWCOSUmJ3c3YPh3dy1


# app 1
def invoke_llm(llm, prompt):
    # print(llm)
    response = llm.invoke(input=prompt)
    return response

"""--------- Open versio ------------
def invoke_llm(llm, prompt):
    prompt = (
        "Please briefly answer the following question:\n"
        f"Question: {prompt}\n"
        "Answer:"
    )
    return llm.invoke(prompt, temperature=0, max_tokens=20)
---------------------------------"""


# app 2
def get_calc_template(Templ):
    prompt_text = """You are a calculator. Evaluate the following math expression.
   
    You have **ONLY** two possible answers:
    1. The result in this exact format: `{expr} = result`
    2. The exact text: `not an expression`

    Remember you are a calculator. Do not give anything other than valid answer.

    Valid answers are like:
    - 1 + 2 = 3
    - sqrt 9 = 3
    - 3 * 2 = 6
    - 7 - 3 = 4
    - 10 / 2 = 5
    - not an expression

    Do NOT add anything after valid answer!
    Do NOT explain your answer! 

    Expression: {expr}"""
    return Templ(template=prompt_text)


def invoke_llm_tmpl(llm, tmpl, expr):
    prompt = tmpl.format(expr=expr)
    response = llm.invoke(input=prompt)
    return response

# app 3
def get_chain(Chain, llm, tmpl ):
    response = Chain(llm=llm, prompt=tmpl) 
    return response


def invoke_chain(chain, params):
    chain_dict = chain.invoke(params)
    response = chain_dict['text']
    return response

# app 4
def get_calc_template_ctx(Templ):
    response = Templ(
        input_variables=["expr","functions"],
        template="""
        ## Instructions
        1. Calculate given {expr} using {functions}
        2. Save result
        3. Replace parameters a and b with values in {expr}
        4. Format answer as given in {functions}:example:return_value
        5. Answer only return_value
        """ 
        )
    return response

def get_calc_context():
    response = {
        "functions": [
            {
                "name": "hyp",
                "description": "Calculates the hypotenuse of a right-angled triangle given sides 'a' and 'b'.",
                "parameters": ["a", "b"],
                "logic": "sqrt(a^2 + b^2)",  
                "example": {"parameters": [3, 4], "return_value": "sqrt(3^2 + 4^2) = 5"}
            }
        ]
    }
    return response

# app 5
def get_chat_template(Templ):
    response = Templ(
        input_variables=["question"],
        template="""
        Chat History: {chat_history}
        User: {question}
        Cohere:
        ## Instructions
        1. This is conversation between me as a human and you as an AI. 
        2. Read {question}
        3. Answer to {question} with one sentence. 
        """ 
        )
    return response


def get_chat_memory(ChatMemory):
    memory = ChatMemory(memory_key='chat_history', return_messages=True)
    return memory


def get_chain_mem(Chain, llm, tmpl, chat_memory):
    return Chain(llm=llm, prompt=tmpl, memory=chat_memory)

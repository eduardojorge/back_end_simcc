import torch
from transformers import pipeline


def Question(messages: str = None, model: str = None):
    if messages == None:
        messages = [
            {
                "role": "system",
                "content": "Você é um chatbot chamado Maria, e você faz listagens e consultas sobre temas academicos",
            },
            {
                "role": "user",
                "content": "Quais termos estão relacionados ao sars Cov 2?",
            },
        ]
    if model == None:
        model = "TinyLlama/TinyLlama-1.1B-Chat-v1.0"
    pipe = pipeline(
        "text-generation",
        model=model,
        torch_dtype=torch.bfloat16,
        device_map="auto",
    )
    prompt = pipe.tokenizer.apply_chat_template(
        messages, tokenize=False, add_generation_prompt=True
    )
    outputs = pipe(
        prompt,
        max_new_tokens=256,
        do_sample=True,
        temperature=0.5,
        top_k=50,
        top_p=0.95,
    )
    return str(outputs[0]["generated_text"])


if __name__ == "__main__":
    print(Question(model="TinyLlama/TinyLlama-1.1B-Chat-v1.0"))

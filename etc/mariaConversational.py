# Use a pipeline as a high-level helper
from transformers import AutoTokenizer, AutoModelForCausalLM, AutoConfig
import torch

hugModel = "PygmalionAI/pygmalion-6b"

mariaTokenizer = AutoTokenizer.from_pretrained(hugModel)
mariaModel = AutoModelForCausalLM.from_pretrained(hugModel)


# Let's chat for 5 lines
for step in range(5):
    # encode the new user input, add the eos_token and return a tensor in Pytorch
    new_user_input_ids = mariaTokenizer.encode(
        input(">> User:") + mariaTokenizer.eos_token, return_tensors="pt"
    )

    # append the new user input tokens to the chat history
    bot_input_ids = (
        torch.cat([chat_history_ids, new_user_input_ids], dim=-1)
        if step > 0
        else new_user_input_ids
    )

    # generated a response while limiting the total chat history to 1000 tokens,
    chat_history_ids = mariaModel.generate(
        bot_input_ids, max_length=1000, pad_token_id=mariaTokenizer.eos_token_id
    )

    # pretty print last ouput tokens from bot
    print(
        "DialoGPT: {}".format(
            mariaTokenizer.decode(
                chat_history_ids[:, bot_input_ids.shape[-1] :][0],
                skip_special_tokens=True,
            )
        )
    )
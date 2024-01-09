response = """
<|system|>\nVoc\u00ea \u00e9 um chatbot chamado Maria, e voc\u00ea faz listagens e consultas sobre temas academicos</s>\n<|user|>\nQuais termos est\u00e3o relacionados ao sars Cov 2?</s>\n<|assistant|>\nSure! Here are some related terms for SARS-CoV-2:\n\n1. Virus: The virus that causes COVID-19 is known as SARS-CoV-2.\n\n2. Spike protein: This is the protein that the virus uses to enter and infect cells.\n\n3. Coronavirus: This is the name given to the virus family that includes SARS-CoV-2.\n\n4. COVID-19: This is the name given to the disease caused by SARS-CoV-2.\n\n5. COVID-19 pandemic: This is the term used to describe the global outbreak of COVID-19.\n\n6. Vaccine: A vaccine is a medicine or a treatment that can help prevent or treat COVID-19.\n\n7. Antiviral: Antiviral medicines can help prevent or treat COVID-19.\n\n8. Mask: A mask is a piece of cloth or fabric worn over the nose and mouth to prevent the spread of viruses.\n\n9. Hand hygiene: Hand hygiene involves washing hands with soap and
"""


fullJson = list()
for sliceResponse in response.split("</s>"):
    chat = sliceResponse.split("|>")

    chat = {f"{chat[0][3:]}": f"{chat[1]}"}
    fullJson.append(chat)

print(fullJson)

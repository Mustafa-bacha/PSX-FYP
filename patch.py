import re

with open("backend/rag_chatbot/prompt_template.py", "r") as f:
    text = f.read()

text = text.replace(
    "Analyze the retrieved context and provide a DETAILED investment analysis:",
    "If the user question is about the market or the stock, analyze the retrieved context and provide a DETAILED investment analysis using the structure below. If it is an unrelated conversational query, apply the escape hatch."
)
text = text.replace(
    "Provide a DETAILED forward-looking analysis based strictly on the data:",
    "If the user question is about the market or the stock, provide a DETAILED forward-looking analysis based strictly on the data using the structure below. If it is an unrelated conversational query, apply the escape hatch."
)
text = text.replace(
    "Provide a COMPREHENSIVE historical analysis:",
    "If the user question is about the market or the stock, provide a COMPREHENSIVE historical analysis using the structure below. If it is an unrelated conversational query, apply the escape hatch."
)
text = text.replace(
    "Provide a DETAILED sentiment and news analysis:",
    "If the user question is about the market or the stock, provide a DETAILED sentiment and news analysis using the structure below. If it is an unrelated conversational query, apply the escape hatch."
)
text = text.replace(
    "Provide a DETAILED risk analysis:",
    "If the user question is about the market or the stock, provide a DETAILED risk analysis using the structure below. If it is an unrelated conversational query, apply the escape hatch."
)
text = text.replace(
    "Analyze the retrieved context and provide a THOROUGH response covering all available data:",
    "If the user question is about the market or the stock, analyze the retrieved context and provide a THOROUGH response covering all available data using the structure below. If it is an unrelated conversational query, apply the escape hatch."
)

base_rules_orig = """_BASE_RULES = \"\"\"CRITICAL RULES — you MUST follow these:
1. ONLY use data from the "Retrieved Context" below. Do NOT use outside knowledge.
2. Cite SPECIFIC numbers: prices, percentages, dates, volumes, sentiment scores.
3. If the context lacks data for a section, write "Insufficient data available" — do NOT make up information.
4. Explain what the numbers MEAN for the investor, not just list them.
5. End with: 'This is educational information, not financial advice.'
\"\"\""""

base_rules_new = """_BASE_RULES = \"\"\"CRITICAL RULES — you MUST follow these:
1. ONLY use data from the "Retrieved Context" below. Do NOT use outside knowledge.
2. Cite SPECIFIC numbers: prices, percentages, dates, volumes, sentiment scores.
3. If the context lacks data for a section, write "Insufficient data available" — do NOT make up information.
4. Explain what the numbers MEAN for the investor, not just list them.
5. End with: 'This is educational information, not financial advice.'
6. ESCAPE HATCH: If the user's question is a simple greeting (like "how are you"), conversational (like "what's up"), general knowledge (like "capital of pakistan", "president of america"), math ("2+2"), or otherwise COMPLETELY UNRELATED to the PSX / stock analysis, YOU MUST REFUSE TO ANALYZE. Instead, politely reply: 'I am focused on PSX market analysis, so I cannot answer general or unrelated questions. How can I help you with stock data today?' and DO NOT output any of the headers below.
\"\"\""""

text = text.replace(base_rules_orig, base_rules_new)

with open("backend/rag_chatbot/prompt_template.py", "w") as f:
    f.write(text)

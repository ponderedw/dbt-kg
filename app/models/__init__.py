import os
model_type, model_id = os.environ['LLM_MODEL_ID'].split(':', 1)

if model_type == 'bedrock':
    from app.models.inference.bedrock_model import ChatBedrock
    ChatModel = ChatBedrock
elif model_type == 'antropic':
    from app.models.inference.antropic_model import ChatAnthropic
    ChatModel = ChatAnthropic
elif model_type == 'openai':
    from app.models.inference.openai_model import ChatOpenAI
    ChatModel = ChatOpenAI

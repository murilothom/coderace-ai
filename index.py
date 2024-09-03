import logging
from flask import Flask
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import google.generativeai as genai
from bson import ObjectId
from unidecode import unidecode
from apscheduler.schedulers.background import BackgroundScheduler
import atexit

app = Flask(__name__)

# Configuração básica de logging
logging.basicConfig(level=logging.INFO)

load_dotenv()

# Configurar a chave da API
genai.configure(api_key=os.getenv('API_KEY'))

# Conexão com o MongoDB
mongo_uri = os.getenv('DB_URI')
client = MongoClient(mongo_uri)
db = client['test']  # Nome do seu banco de dados
feedbacks_collection = db['feedbacks']  # Coleção de feedbacks
questions_collection = db['questions']  # Coleção de questões
results_collection = db['results']  # Nova coleção para armazenar resultados

def calcular_media_por_setor(sector: str):
    logging.info(f"Calculando média para o setor: {sector}")
    resultados = {}

    documentos_setor = list(feedbacks_collection.find({'sector': sector}))

    if not len(documentos_setor) > 0:
        raise ValueError("Setor inválido")

    notas_por_question = {}

    for documento in documentos_setor:
        respostas = documento.get('answers', [])
        for resposta in respostas:
            question_id = resposta['questionId']
            rate = resposta.get('rate')

            try:
                question_object_id = ObjectId(question_id)
                question = questions_collection.find_one({"_id": question_object_id})
            except Exception as e:
                logging.error(f"Erro ao converter questionId para ObjectId: {e}")
                question = None

            question_text = question['question'] if question else "Descrição não encontrada"
            journey = question['journey'] if question else "Jornada desconhecida"

            chave = f"{question_text} ({journey})"

            if chave not in notas_por_question:
                notas_por_question[chave] = {'soma': 0, 'contador': 0}

            if rate is not None:
                notas_por_question[chave]['soma'] += rate
                notas_por_question[chave]['contador'] += 1

    medias_por_question = {}
    for chave, valores in notas_por_question.items():
        soma = valores['soma']
        contador = valores['contador']
        medias_por_question[chave] = soma / contador if contador != 0 else 0

    resultados[sector] = medias_por_question

    results_collection.update_one({'sector': sector}, {'$set': {'results': resultados}}, upsert=True)

    return resultados

def processar_respostas():
    logging.info("Processando respostas...")
    setores = feedbacks_collection.distinct('sector')
    for setor in setores:
        try:
            resultados = calcular_media_por_setor(setor)
            dados_json = results_collection.find_one({'sector': setor})

            if not dados_json:
                logging.warning(f"Resultados não encontrados para o setor {setor}.")
                continue

            base_prompt = "Seu prompt base aqui."
            respostas_por_setor = {}

            for setor, medias in dados_json['results'].items():
                insights = []
                for question_journey, media in medias.items():
                    question, journey = question_journey.split(" (")
                    journey = journey.rstrip(")")
                    insights.append(f"- {question} (Jornada: {journey}): Média de {media:.2f}")

                prompt = f"{base_prompt}\n\n**Setor: {setor}**\n\n" + "\n".join(insights)

                model = genai.GenerativeModel('gemini-1.5-flash')
                response = model.generate_content(prompt)

                resposta_sem_acento = unidecode(response.text)
                respostas_por_setor[setor] = resposta_sem_acento

            results_collection.update_one({'sector': setor}, {'$set': {'responses': respostas_por_setor}}, upsert=True)
            logging.info(f"Resultados processados para o setor {setor}.")

        except ValueError as e:
            logging.error(f"Erro ao processar setor {setor}: {e}")

logging.info("Iniciando o scheduler...")
scheduler = BackgroundScheduler()
scheduler.add_job(func=processar_respostas, trigger="interval", minutes=60)
scheduler.start()

atexit.register(lambda: scheduler.shutdown())

if __name__ == "__main__":
    logging.info("Iniciando a aplicação Flask...")
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)

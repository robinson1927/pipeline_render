from flask import Flask
from pipeline import ejecutar_pipeline
import os



app = Flask(__name__)

@app.route("/")
def home():
    return "Servicio activo"

@app.route("/run")
def run():
    ejecutar_pipeline()
    return "Pipeline ejecutado"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
from flask import Flask
import sys

app = Flask(__name__)

serverId = None

@app.route("/home")
def home():
    response = {"message": f"Hello from Server: {serverId}", "status": "successful"}
    return response, 200


@app.route("/heartbeat")
def heartbeat():
    return {}, 200


if __name__ == "__main__":
    serverId = sys.argv[1]
    app.run(debug=True, host="0.0.0.0", port=0)

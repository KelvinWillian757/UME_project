import json
from flask import Flask, request, jsonify
from google.cloud import pubsub_v1

app = Flask(__name__)

PROJECT_ID = "assertiv"
TOPIC_ID = "credit-evaluations-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


@app.route("/", methods=["POST"])
def receive_webhook():
    try:
        body = request.get_json(silent=True)

        if not body:
            return jsonify({"error": "empty body"}), 400

        payload_bytes = json.dumps(body, ensure_ascii=False).encode("utf-8")

        future = publisher.publish(topic_path, payload_bytes)
        message_id = future.result()

        return jsonify({
            "status": "published",
            "pubsub_message_id": message_id
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
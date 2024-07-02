from flask import Flask, request, jsonify, abort
import psycopg2
import uuid

app = Flask(__name__)

DATABASE_URL = "postgresql://postgres:1234@localhost/postgres"

@app.route('/')
def home_page():
    return "Just Default Home Page!"

def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    return conn

@app.route("/contacts", methods=["GET"])
def get_contacts():
    params = request.args
    skip = int(params.get('skip', 0))
    limit = int(params.get('limit', 10))
    name = params.get('name')
    email = params.get('email')
    category = params.get('category')
    sort_by = params.get('sort_by')

    query = "SELECT * FROM contacts WHERE TRUE"
    query_params = []
    if name:
        query += " AND name ILIKE %s"
        query_params.append(f"%{name}%")
    if email:
        query += " AND email ILIKE %s"
        query_params.append(f"%{email}%")
    if category:
        query += " AND category_id IN (SELECT id FROM categories WHERE label ILIKE %s)"
        query_params.append(f"%{category}%")
    if sort_by == "created":
        query += " ORDER BY created_at DESC"
    query += " LIMIT %s OFFSET %s"
    query_params.extend([limit, skip])

    conn = get_db()
    cur = conn.cursor()
    cur.execute(query, query_params)
    contacts = cur.fetchall()
    cur.close()
    conn.close()

    response = [
        dict(zip([col[0] for col in cur.description], row))
        for row in contacts
    ]
    return jsonify(response)

@app.route("/contacts", methods=["POST"])
def create_contact():
    contact = request.get_json()

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO contacts (name, email, phone, category_id) VALUES (%s, %s, %s, %s) RETURNING id",
        (contact['name'], contact['email'], contact['phone'], contact['category_id'])
    )
    contact_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()

    contact['id'] = contact_id
    return jsonify(contact), 201

@app.route("/contacts/<contact_id>", methods=["GET"])
def get_contact(contact_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM contacts WHERE id = %s", (contact_id,))
    contact = cur.fetchone()
    cur.close()
    conn.close()

    if contact is None:
        abort(404, "Contact not found")
    else:
        response = dict(zip([col[0] for col in cur.description], contact))
        return jsonify(response)

@app.route("/contacts/<contact_id>", methods=["DELETE"])
def delete_contact(contact_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM contacts WHERE id = %s RETURNING id", (contact_id,))
    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"message": "Contact deleted successfully"})

@app.route("/contacts/<contact_id>", methods=["PATCH"])
def update_contact(contact_id):
    contact_update = request.get_json()

    conn = get_db()
    cur = conn.cursor()
    update_data = {k: v for k, v in contact_update.items() if v is not None}
    query = "UPDATE contacts SET " + ", ".join(f"{k} = %s" for k in update_data.keys()) + " WHERE id = %s RETURNING id"
    params = list(update_data.values()) + [contact_id]
    cur.execute(query, params)
    conn.commit()
    cur.close()
    conn.close()

    return jsonify({**update_data, "id": contact_id})

@app.route("/categories", methods=["GET"])
def get_categories():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM categories")
    categories = cur.fetchall()
    cur.close()
    conn.close()

    response = [
        dict(zip([col[0] for col in cur.description], row))
        for row in categories
    ]
    return jsonify(response)

@app.route("/categories", methods=["POST"])
def create_category():
    category = request.get_json()

    conn = get_db()
    cur = conn.cursor()
    cur.execute("INSERT INTO categories (label) VALUES (%s) RETURNING id", (category['label'],))
    category_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()

    category['id'] = category_id
    return jsonify(category), 201

@app.route("/categories/<category_id>", methods=["GET"])
def get_category(category_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM categories WHERE id = %s", (category_id,))
    category = cur.fetchone()
    cur.close()
    conn.close()

    if category is None:
        abort(404, "Category not found")
    else:
        response = dict(zip([col[0] for col in cur.description], category))
        return jsonify(response)

@app.route("/categories/<category_id>", methods=["DELETE"])
def delete_category(category_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM categories WHERE id = %s RETURNING id", (category_id,))
    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"message": "Category deleted successfully"})

@app.route("/categories/<category_id>", methods=["PATCH"])
def update_category(category_id):
    category_update = request.get_json()

    conn = get_db()
    cur = conn.cursor()
    update_data = {k: v for k, v in category_update.items() if v is not None}
    query = "UPDATE categories SET " + ", ".join(f"{k} = %s" for k in update_data.keys()) + " WHERE id = %s RETURNING id"
    params = list(update_data.values()) + [category_id]
    cur.execute(query, params)
    conn.commit()
    cur.close()
    conn.close()

    return jsonify({**update_data, "id": category_id})

if __name__ == "__main__":
    app.run(debug=True, port=5000)

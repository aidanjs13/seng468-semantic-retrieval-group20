from flask import Flask, request, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
import psycopg
from psycopg import errors
import jwt
from datetime import datetime, timedelta, timezone
import os
import uuid
from pgvector.psycopg import register_vector
from sentence_transformers import SentenceTransformer
import pymupdf
import re

from miniostorage import init_minio_bucket, upload_pdf, get_pdf, delete_pdf


app = Flask(__name__)


secret = os.getenv("JWT_SECRET")
db_url = os.getenv("DATABASE_URL")
UPLOADDIR = "uploads"
vector_embed = SentenceTransformer("all-MiniLM-L6-v2")


# first 4 functions are db helpers specifically
# can be moved to separate files as the project is expanded

# initialize the database table if it doesn't yet exist
def init_db():
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id SERIAL PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    document_id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(user_id),
                    filename TEXT NOT NULL,
                    stored_path TEXT NOT NULL,
                    status TEXT NOT NULL,
                    upload_date TIMESTAMP
                )
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS doc_chunks (
                chunk_id TEXT PRIMARY KEY,
                document_id TEXT NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
                user_id INTEGER NOT NULL REFERENCES users(user_id),
                text TEXT NOT NULL,
                embedding VECTOR(384)
                )
            """)
            

# Queries db for a user
# returns None or a dict containing the tuple's contents
# Returns the user data
def get_user(username):
    # query database
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT user_id, username, password_hash
                FROM users
                WHERE username = %s
            """, (username,))
            row = cur.fetchone()

    # didn't find user matching key, so return none
    if row is None:
        return None
    
    # return the user data
    return {
        "user_id": row[0],
        "username": row[1],
        "password_hash": row[2]
    }

# No explicit error check.
# Just errors on failed insert, and route function handles what to do after
# Return the inserted user id
def signup_user(username, pw_hash):
    # insert to database
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (username, password_hash)
                VALUES (%s, %s)
                RETURNING user_id
            """, (username, pw_hash))
            row = cur.fetchone()
        conn.commit()

    return row[0]

# use pyjwt for generating a token
def token_gen(username, uid):
    # create payload for token encoding
    payload = {
            "sub": str(uid),
            "username": username,
            "exp": datetime.now(timezone.utc) + timedelta(hours=24)
        }
    return jwt.encode(payload, secret, algorithm="HS256")


def getUserIdFromToken():
    #token value
    authHeader =request.headers.get("Authorization")
    if authHeader == None:
        return None
    if not (authHeader[0:7] == "Bearer "):
        return None

    #getting everything after the space (the token)
    token= authHeader.split(" ", 1)[1]
    try:
        payload = jwt.decode(token, secret, algorithms=["HS256"])
        return int(payload["sub"])
    except:
        print("error in the token")
        return None

def insertDocument(document_id, user_id, filename, stored_path, status):
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO documents (document_id, user_id, filename, stored_path, status) VALUES (%s, %s, %s, %s, %s)""", (document_id, user_id, filename, stored_path, status))
        conn.commit()

# placeholder search (may be moved to worker depending on final structure)
# we CAN reuse this later
def get_doc_by_user(uid):
    # get users documents
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT document_id, filename, stored_path
                FROM documents
                WHERE user_id = %s
            """, (uid,))
            rows = cur.fetchall()

    # return document info for parser
    return [
        {
            "doc_id": row[0],
            "filename": row[1],
            "path": row[2]
        }
        for row in rows
    ]

def get_documents_list(uid):
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT document_id, filename, upload_date, status
                FROM documents
                WHERE user_id = %s
                ORDER BY upload_date DESC
            """, (uid,))
            rows = cur.fetchall()

    return [
        {
            "document_id": row[0],
            "filename": row[1],
            "upload_date": row[2],
            "status": row[3],
            "page_count": None
        }
        for row in rows
    ]


def get_document_by_id(uid, document_id):
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT document_id, filename, stored_path, status
                FROM documents
                WHERE user_id = %s AND document_id = %s
            """, (uid, document_id))
            row = cur.fetchone()

    if row is None:
        return None

    return {
        "document_id": row[0],
        "filename": row[1],
        "stored_path": row[2],
        "status": row[3]
    }



def delete_document_from_db(uid, document_id):
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM documents
                WHERE user_id = %s AND document_id = %s
            """, (uid, document_id))
        conn.commit()




# This is named pdf_to_paragraphs, but it really is to blocks
# where blocks are defined by the PyMuPDF library
# this seemed adequate for the checkpoint
def pdf_to_paragraphs(pdf):
    try:
        this_pdf = pymupdf.open(pdf)
    except Exception:
        return []
    # this function does NOT clean the text yet
    # we may have to modify the logic for this to get nicer paragraphs
    # just grabs "blocks", defined according to PyMuPDF library
    block_text = []
    for page in this_pdf:
        blocks = page.get_text("blocks")

        block_text += [block[4] for block in blocks]
    
    this_pdf.close()

    return block_text


def insert_to_vectordb(uid, document_id, pdf_name):
    # turn pdf to blocks using the above helper
    os.makedirs(UPLOADDIR, exist_ok = True)
    tempfilepath = os.path.join(UPLOADDIR, f"{document_id}.pdf")

    # try block is for inserting to vector db
    try:
        get_pdf(pdf_name, tempfilepath)

        blocks_to_insert = pdf_to_paragraphs(tempfilepath)
        if len(blocks_to_insert) == 0:
            return
        
        # turn the PyMuPDF blocks to vectors
        vectors = vector_embed.encode(blocks_to_insert, normalize_embeddings=True)

        # insert to vector db
        with psycopg.connect(db_url) as conn:
            register_vector(conn)

            with conn.cursor() as cur:
                # this first query is just for stability, not sure if necessary

                # in the case that we for some reason redo an insert, just remove
                # the old one
                cur.execute("""
                    DELETE FROM doc_chunks
                    WHERE document_id = %s
                """, (document_id,))

                # inserts the chunks and associated text

                # stores a unique ID for each chunk, as well as information
                # such as user and document
                for block, vector in zip(blocks_to_insert, vectors):
                    cur.execute("""
                        INSERT INTO doc_chunks (
                            chunk_id,
                            document_id,
                            user_id,
                            text,
                            embedding
                        )
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        str(uuid.uuid4()),
                        document_id,
                        uid,
                        block,
                        vector.tolist()
                    ))
            conn.commit()
    # we need to guarantee that the temp file is removed
    # so that is what the finally block is for
    finally:
        if os.path.exists(tempfilepath):
            os.remove(tempfilepath)




def search_chunks_by_embedding(uid, query):
    # convert query to embedding vector
    query_vector = vector_embed.encode(query, normalize_embeddings=True)

    with psycopg.connect(db_url) as conn:
        register_vector(conn)

        with conn.cursor() as cur:
            cur.execute("""
                SELECT dc.text, dc.document_id, d.filename,
                       dc.embedding <=> %s::vector AS distance
                FROM doc_chunks dc
                JOIN documents d
                  ON dc.document_id = d.document_id
                WHERE dc.user_id = %s
                ORDER BY dc.embedding <=> %s::vector
                LIMIT 5
            """, (query_vector.tolist(), uid, query_vector.tolist()))

            rows = cur.fetchall()

    return [
        {
            "text": row[0],
            "score": 1-float(row[3]),  #higher means better match
            "document_id": row[1],
            "filename": row[2]
        }
        for row in rows
    ]


####################################
# API ENDPOINTS
####################################

# SIGNUP
# 200 on SUCCESS
# 409 on duplicate
# DOES NOT HANDLE INVALID INPUT (yet)
@app.post("/auth/signup")
def signup():
    
    # request to json
    data = request.get_json()

    # retrieve username and password from request
    username = data.get("username")
    password = data.get("password")

    # uses the workzeug password hash function
    pw_hash = generate_password_hash(password)

    # try/except block for signup
    # signup will error if duplicate
    try:
        user_id = signup_user(username, pw_hash)
    except errors.UniqueViolation:
        return jsonify({"error": "Username already exists"}), 409

    # return user id along with 200 success
    return jsonify({
        "message": "User created successfully",
        "user_id": user_id
    }), 200

# LOGIN
# 200 on SUCCESS
# 401 on unauthorized
# DOES NOT HANDLE INVALID INPUT (yet)
@app.post("/auth/login")
def login():

    # request to json
    data = request.get_json()

    # retrieve username and password from request
    username = data.get("username")
    password = data.get("password")

    # get user info
    user = get_user(username)

    if user is None:
        return jsonify({"error": "Invalid credentials"}), 401

    # uses the workzeug password check function
    if not check_password_hash(user["password_hash"], password):
        return jsonify({"error": "Invalid credentials"}), 401

    # uses our token helper
    token = token_gen(user["username"], user["user_id"])

    # return token and user id along with 200 success
    return jsonify({
        "token": token,
        "user_id": user["user_id"]
    }), 200


@app.post("/documents")
def upload_document():
    user_id = getUserIdFromToken()

    if user_id == None:
        return jsonify({"login first": "Unauthorized"}), 401

    #get users file
    uploadedFile = request.files["file"]

    document_id = str(uuid.uuid4())
    stored_filename = f"{document_id}_{uploadedFile.filename}"
    stored_path = f"{user_id}/{stored_filename}"

    upload_pdf(uploadedFile, stored_path)

    insertDocument(
        document_id=document_id,
        user_id=user_id,
        filename=uploadedFile.filename,
        stored_path=stored_path,
        status="processing"
    )

    #### TEMPORARY #####
    # This is for testing purposes of inserting to the vector DB
    # Likely to be moved as we integrate celery
    insert_to_vectordb(user_id, document_id, stored_path)


    return jsonify({
        "message": "PDF uploaded, processing started",
        "document_id": document_id,
        "status": "processing"
    }), 202


@app.get("/documents")
def get_documents():
    user_id = getUserIdFromToken()

    if user_id is None:
        return jsonify({"error": "Unauthorized"}), 401

    user_docs = get_documents_list(user_id)

    return jsonify(user_docs), 200


@app.delete("/documents/<document_id>")
def delete_document(document_id):
    user_id = getUserIdFromToken()

    if user_id is None:
        return jsonify({"error": "Unauthorized"}), 401

    doc = get_document_by_id(user_id, document_id)

    if doc is None:
        return jsonify({"error": "Document not found"}), 404

    delete_pdf(doc["stored_path"])

    delete_document_from_db(user_id, document_id)

    return jsonify({
        "message": "Document deleted successfully",
        "document_id": document_id
    }), 200


# TEMPORARY search function
# will need to rework this as scoring is added
# currently just grabs first 5 paragraphs containing the query
@app.get("/search")
def search():
    user_id = getUserIdFromToken()
    if user_id is None:
        return jsonify({"error": "Unauthorized"}), 401

    search_query = (request.args.get("q") or "").strip()

    if search_query == "":
        return jsonify([]), 200

    results = search_chunks_by_embedding(user_id, search_query)

    return jsonify(results), 200

if __name__ == "__main__":
    # initialize db then expose on port 8080
    init_db()
    init_minio_bucket()
    app.run(host="0.0.0.0", port=8080, debug=True)
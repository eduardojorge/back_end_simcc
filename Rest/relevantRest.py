import os
from flask import Blueprint, send_file, request
from Dao import relevantSQL

management = Blueprint("managementRest", __name__)

UPLOAD_FOLDER = os.path.join(os.getcwd(), "Files", "image_productions")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@management.route('/image/<id>', methods=["POST"])
def post_image(id):
    type_ = request.args.get('type')

    if "file" not in request.files:
        return "No files sent", 400

    file = request.files["file"]
    if file.filename is None:
        return "No files selected", 400
    
    if "." in file.filename:
        extension = file.filename.rsplit(".", 1)[1]  
    else:
        extension = "jpg"

    file_path = os.path.join(UPLOAD_FOLDER, f"{id}.{extension}")
    relevantSQL.add_relevant_production(id, type_, True)
    file.save(file_path)
    return "Image saved successfully", 200

@management.route("/image/<id>", methods=["GET"])
def get_image(id):
    for extension in ["jpg", "jpeg", "png", "gif"]:
        file_path = os.path.join(UPLOAD_FOLDER, f"{id}.{extension}")
        if os.path.isfile(file_path):
            return send_file(file_path)
        
@management.route("/image/<id>", methods=["DELETE"])
def delete_image(id):
    type_ = request.args.get('type')
    for extension in ["jpg", "jpeg", "png", "gif"]:
        file_path = os.path.join(UPLOAD_FOLDER, f"{id}.{extension}")
        if os.path.isfile(file_path):
            os.remove(file_path)
            relevantSQL.delete_relevant_production(id, type_)
            return "Image deleted successfully", 200
    return "Image not found", 404

@management.route("/relevant/<id>", methods=["POST"])
def post_relevant_production(id):
    type_ = request.args.get('type')
    relevantSQL.add_relevant_production(id, type_, False)
    return 'OK', 200

@management.route("/relevant", methods=["GET"])
def get_relevant_production_list():
    relevant_list = relevantSQL.get_relevant_list()
    return relevant_list

@management.route("/relevant/<id>", methods=["DELETE"])
def delete_relevant_production(id):
    type_ = request.args.get('type')
    relevantSQL.delete_relevant_production(id, type_)
    return 'OK', 200
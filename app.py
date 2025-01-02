import os
import json
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from werkzeug.utils import secure_filename
import zipfile
from pathlib import Path
from schema import DatabaseMetadataGenerator
from sharding import DatabaseShardGenerator
from queryexecution import DistributedQueryEngine


app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads/'
app.config['ALLOWED_EXTENSIONS'] = {'zip', 'tar', 'tar.gz'}
app.secret_key = 'your_secret_key'

COMPANY_LIST_FILE = 'companies.json'

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1] in app.config['ALLOWED_EXTENSIONS']

def load_companies():
    if os.path.exists(COMPANY_LIST_FILE):
        with open(COMPANY_LIST_FILE, 'r', encoding='utf-8') as file:
            data = json.load(file)
            return data.get('companies', [])
    return []

def save_companies(companies_list):
    with open(COMPANY_LIST_FILE, 'w', encoding='utf-8') as file:
        json.dump({'companies': companies_list}, file, ensure_ascii=False)

@app.route('/')
def index():
    companies = load_companies()
    return render_template('index.html', companies=companies)

def extract_folder(zip_file_path, company_folder):
    try:
        original_data_folder = os.path.join(company_folder, 'originaldata')
        os.makedirs(original_data_folder, exist_ok=True)
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(original_data_folder)
        return True
    except zipfile.BadZipFile:
        raise Exception("Invalid zip file")
    except Exception as e:
        raise Exception(f"Error extracting zip: {str(e)}")

@app.route('/upload', methods=['GET', 'POST'])
def upload():
    if request.method == 'POST':
        if 'file' not in request.files or not request.form.get('company_name'):
            flash('Missing file or company name')
            return redirect(url_for('index'))

        file = request.files['file']
        company_name = request.form['company_name'].strip()

        if file.filename == '' or not company_name:
            flash('No selected file or company name')
            return redirect(url_for('index'))

        if not file.filename.endswith('.zip'):
            flash('Only ZIP files are allowed')
            return redirect(url_for('index'))

        try:
            folder_name = f"{company_name}_folder"
            company_path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
            os.makedirs(company_path, exist_ok=True)
            
            zip_path = os.path.join(company_path, secure_filename(f"{company_name}.zip"))
            file.save(zip_path)
            extract_folder(zip_path, company_path)
            
            companies = load_companies()
            company_data = {
                "company_name": company_name,
                "folder_path": os.path.join(company_path, 'originaldata'),
                "status": "Uploaded"
            }
            
            if company_data not in companies:
                companies.append(company_data)
                save_companies(companies)

            flash(f"Successfully uploaded and extracted data for {company_name}")
            return redirect(url_for('index'))

        except Exception as e:
            flash(f'Error: {str(e)}')
            return redirect(url_for('index'))

    return render_template('upload.html')

@app.route('/company/<company_name>')
def company_page(company_name):
    companies = load_companies()
    company = next((c for c in companies if c['company_name'] == company_name), None)
    
    if company is None:
        flash('Company not found')
        return redirect(url_for('index'))
    
    if company['status'] == "Sharded and ready to run query":
        company_folder = os.path.join(app.config['UPLOAD_FOLDER'], f"{company_name}_folder")
        schema_metadata_file = os.path.join(company_folder, 'schema_metadata.json')
        shard_metadata_file = os.path.join(company_folder, 'shard_metadata.json')
        
        try:
            with open(schema_metadata_file, 'r') as f:
                company['schema_metadata'] = json.dumps(json.load(f), indent=2)
        except FileNotFoundError:
            company['schema_metadata'] = "Schema metadata file not found."
        
        try:
            with open(shard_metadata_file, 'r') as f:
                company['sharding_metadata'] = json.dumps(json.load(f), indent=2)
        except FileNotFoundError:
            company['sharding_metadata'] = "Sharding metadata file not found."
    
    return render_template('company.html', company=company)

@app.route('/shard/<company_name>', methods=['POST'])
def shard_data(company_name):
    companies = load_companies()
    company = next((c for c in companies if c['company_name'] == company_name), None)
    
    if company is None:
        return jsonify({'error': 'Company not found'}), 404
    
    try:
        input_folder = company['folder_path']
        output_folder = os.path.join(app.config['UPLOAD_FOLDER'],f"{company_name}_folder", f"{company_name}_sharded")
        output_folder2 = os.path.join(app.config['UPLOAD_FOLDER'],f"{company_name}_folder")
        os.makedirs(output_folder, exist_ok=True)
        shard_count = 20  # You can make this configurable if needed

        # Generate schema metadata
        schema_metadata_file = os.path.join(output_folder2, 'schema_metadata.json')
        schema_metadata_generator = DatabaseMetadataGenerator(input_folder, schema_metadata_file)
        schema_metadata_generator.generate()

        # Shard the database
        shard_metadata_file = os.path.join(output_folder2, 'shard_metadata.json')
        shard_generator = DatabaseShardGenerator(input_folder, output_folder,output_folder2, shard_count)
        shard_generator.generate()

        # Update company status and metadata
        company['status'] = "Sharded and ready to run query"
        company['sharded_folder'] = output_folder

        save_companies(companies)

        return jsonify({'status': 'success', 'message': 'Data sharded successfully'})

    except Exception as e:
        print(f"Error in shard_data: {str(e)}")
        return jsonify({'error': str(e)}), 500
    
@app.route('/query_run/<company_name>', methods=['GET', 'POST'])
def query_run(company_name):
    companies = load_companies()
    company = next((c for c in companies if c['company_name'] == company_name), None)
    
    if company is None:
        flash('Company not found')
        return redirect(url_for('index'))
    
    company_folder = os.path.join(app.config['UPLOAD_FOLDER'], f"{company_name}_folder")
    database_path = os.path.join(company_folder, f"{company_name}_sharded")
    schema_metadata_path = os.path.join(company_folder, 'schema_metadata.json')
    shard_metadata_path = os.path.join(company_folder, 'shard_metadata.json')
    
    engine = DistributedQueryEngine(database_path, schema_metadata_path, shard_metadata_path)
    
    if request.method == 'POST':
        query = request.form['query']
        try:
            result_df = engine.execute_query(query)
            result_html = result_df.to_html(classes='table table-striped', index=False)
        except Exception as e:
            flash(f'Error executing query: {str(e)}')
            result_html = None
        
        return render_template('query_run.html', company=company, result_html=result_html, query=query)
    
    # For GET requests, display the schema information
    schema_info = {}
    try:
        with open(schema_metadata_path, 'r') as f:
            schema_data = json.load(f)
        for table_name, table_info in schema_data.items():
            columns = [f"{col['name']} ({col['type']})" for col in table_info['columns']]
            schema_info[table_name] = columns
    except Exception as e:
        flash(f'Error loading schema information: {str(e)}')
    
    return render_template('query_run.html', company=company, schema_info=schema_info)


if __name__ == '__main__':
    app.run(debug=True)
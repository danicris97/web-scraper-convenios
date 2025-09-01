#!/usr/bin/env python3
"""
Extractor de Convenios - Universidad Nacional de Salta
Procesa PDFs de convenios y genera CSV para importar a Laravel
"""

import os
import csv
import re
import requests
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging
from pathlib import Path

# Librerías para OCR y procesamiento de PDF
try:
    import pytesseract
    from PIL import Image
    import pdf2image
    from PyPDF2 import PdfReader
    import fitz  # PyMuPDF
except ImportError as e:
    print(f"Error: Instala las dependencias necesarias: {e}")
    print("pip install pytesseract pillow pdf2image PyPDF2 PyMuPDF requests")
    exit(1)

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('convenio_extractor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ConvenioExtractor:
    def __init__(self, output_dir: str = "convenios_output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Diccionarios para mapear tipos encontrados a enums
        self.tipos_convenio = {
            'acta acuerdo': 'Acta Acuerdo',
            'acta complemento': 'Acta Complemento', 
            'acta compromiso': 'Acta Compromiso',
            'acta especifica': 'Acta Especifica',
            'acuerdo': 'Acuerdo',
            'acuerdo complementario': 'Acuerdo Complementario',
            'acuerdo cooperacion': 'Acuerdo Cooperacion',
            'acuerdo especifico': 'Acuerdo Especifico',
            'acuerdo investigacion': 'Acuerdo Investigacion',
            'adenda': 'Adenda',
            'adhesion': 'Adhesion',
            'anexo': 'Anexo',
            'comision de estudio': 'Comision de Estudio',
            'carta intencion': 'Carta Intencion',
            'especifico': 'Especifico',
            'general': 'General',
            'marco': 'Marco',
            'marco de colaboracion': 'Marco de Colaboracion',
            'marco de cooperacion': 'Marco de Cooperacion',
            'marco de investigacion': 'Marco de Investigacion',
            'marco de intercambio': 'Marco de Intercambio',
            'marco de intercambio de alumnos': 'Marco de Intercambio de Alumnos',
            'marco de pasantias': 'Marco de Pasantias',
            'memorandum': 'Memorandum',
            'memorandum de entendimiento': 'Memorandum de Entendimiento',
            'pps': 'PPS',
            'protocolo': 'Protocolo',
            'protocolo adicional': 'Protocolo Adicional',
            'protocolo de colaboracion': 'Protocolo de Colaboracion',
            'protocolo especifico': 'Protocolo Especifico',
            'protocolo de investigacion': 'Protocolo de Investigacion',
            'proyecto': 'Proyecto',
            'subvencion': 'Subvencion',
            'transferencia tecnologica': 'Transferencia Tecnologica'
        }
        
        self.tipos_institucion = {
            'agencia': 'Agencia',
            'agremiacion': 'Agremiacion', 
            'asociacion': 'Asociacion',
            'club': 'Club',
            'educativa': 'Educativa',
            'empresa': 'Empresa',
            'ente': 'Ente',
            'fundacion': 'Fundacion',
            'gobierno': 'Gobierno',
            'gubernamental': 'Gubernamental',
            'investigadora': 'Investigadora',
            'instituto': 'Instituto',
            'nacional': 'Nacional',
            'municipal': 'Municipal',
            'salud': 'Salud',
            'sindical': 'Sindical',
            'social': 'Social',
            'ong': 'ONG',
            'universitaria': 'Universitaria',
            'universidad': 'Universitaria',
            'provincia': 'Gubernamental',
            'ministerio': 'Gubernamental',
            'direccion': 'Gubernamental'
        }
        
        self.tipos_renovacion = {
            'partes iguales': 'Partes Iguales',
            'escalera': 'Escalera', 
            'unica': 'Unica',
            'sin renovacion': 'Sin Renovacion',
            'renovable': 'Renovable de Comun Acuerdo',
            'renovable de comun acuerdo': 'Renovable de Comun Acuerdo'
        }
        
        self.cargos = {
            'rector': 'Rector/a',
            'rectora': 'Rector/a',
            'vicerrector': 'Vicerrector/a',
            'vicerrectora': 'Vicerrector/a',
            'secretario': 'Secretario/a',
            'secretaria': 'Secretario/a',
            'coordinador': 'Coordinador/a',
            'coordinadora': 'Coordinador/a',
            'decano': 'Decano/a',
            'decana': 'Decano/a',
            'vicedecano': 'Vicedecano/a',
            'vicedecana': 'Vicedecano/a',
            'director': 'Director/a',
            'directora': 'Director/a',
            'investigador': 'Investigador/a',
            'investigadora': 'Investigador/a',
            'ministro': 'Director/a',
            'ministra': 'Director/a',
            'interventor': 'Director/a',
            'interventora': 'Director/a'
        }

    def download_file(self, url: str, filename: str) -> bool:
        """Descarga un archivo desde URL (PDF o HTML)"""
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            filepath = self.output_dir / filename
            
            # Guardar como texto si es HTML, binario si es PDF
            if filename.lower().endswith('.html'):
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(response.text)
            else:
                with open(filepath, 'wb') as f:
                    f.write(response.content)
            
            logger.info(f"Descargado: {filename}")
            return True
        except Exception as e:
            logger.error(f"Error descargando {url}: {e}")
            return False

    def extract_text_from_file(self, file_path: str) -> str:
        """Extrae texto de archivo (PDF o HTML)"""
        
        if file_path.lower().endswith('.html'):
            return self.extract_text_from_html(file_path)
        else:
            return self.extract_text_from_pdf(file_path)
    def extract_text_from_html(self, html_path: str) -> str:
        """Extrae texto de archivo HTML"""
        try:
            with open(html_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Importar BeautifulSoup aquí para que sea opcional
            try:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(content, 'html.parser')
                
                # Remover scripts y estilos
                for script in soup(["script", "style"]):
                    script.decompose()
                
                # Obtener texto limpio
                text = soup.get_text()
                
                # Limpiar espacios y saltos de línea
                lines = (line.strip() for line in text.splitlines())
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                text = '\n'.join(chunk for chunk in chunks if chunk)
                
                return text
            
            except ImportError:
                logger.warning("BeautifulSoup no disponible, usando extracción simple")
                # Fallback: regex simple para remover tags HTML
                import re
                text = re.sub(r'<[^>]+>', '', content)
                text = re.sub(r'\s+', ' ', text)
                return text.strip()
            
        except Exception as e:
            logger.error(f"Error extrayendo texto de HTML: {e}")
            return ""

    def extract_text_from_pdf(self, pdf_path: str) -> str:
        text = ""
        
        # Método 1: Intentar extraer texto directo
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PdfReader(file)
                for page in pdf_reader.pages:
                    page_text = page.extract_text()
                    if page_text.strip():
                        text += page_text + "\n"
        except Exception as e:
            logger.warning(f"Error extrayendo texto directo: {e}")
        
        # Método 2: Si no hay texto o es muy poco, usar OCR
        if len(text.strip()) < 100:
            logger.info("Usando OCR para extraer texto")
            text = self.ocr_pdf(pdf_path)
        
        return text

    def ocr_pdf(self, pdf_path: str) -> str:
        """Extrae texto usando OCR (Tesseract)"""
        try:
            # Convertir PDF a imágenes
            images = pdf2image.convert_from_path(pdf_path, dpi=300)
            
            text = ""
            for i, image in enumerate(images):
                # OCR en cada página
                page_text = pytesseract.image_to_string(image, lang='spa')
                text += f"\n--- PÁGINA {i+1} ---\n{page_text}\n"
                
                if i >= 10:  # Limitar a 10 páginas para evitar procesos muy largos
                    break
            
            return text
        except Exception as e:
            logger.error(f"Error en OCR: {e}")
            return ""

    def extract_resolution_data(self, text: str, url: str) -> Dict:
        """Extrae datos de resolución del texto"""
        resolution_data = {
            'numero': '',
            'fecha': '',
            'tipo': 'DR',
            'expediente_numero': '',
            'expediente_anio': '',
            'dependencia_id': '',
            'link': url
        }
        
        # Extraer número de resolución del URL
        url_match = re.search(r'R-DR-(\d{4})-(\d{3,4})\.pdf', url)
        if url_match:
            resolution_data['expediente_anio'] = url_match.group(1)
            resolution_data['numero'] = url_match.group(2)
        
        # Buscar fecha en el texto
        date_patterns = [
            r'(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})',
            r'(\d{1,2})/(\d{1,2})/(\d{4})',
            r'(\d{4})-(\d{1,2})-(\d{1,2})'
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                if 'de' in pattern:
                    day, month_name, year = match.groups()
                    month_map = {
                        'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
                        'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
                        'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'
                    }
                    month = month_map.get(month_name.lower(), '01')
                    resolution_data['fecha'] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                else:
                    # Formato directo
                    if len(match.groups()) == 3 and len(match.group(1)) == 4:
                        resolution_data['fecha'] = f"{match.group(1)}-{match.group(2).zfill(2)}-{match.group(3).zfill(2)}"
                    else:
                        resolution_data['fecha'] = f"{match.group(3)}-{match.group(2).zfill(2)}-{match.group(1).zfill(2)}"
                break
        
        # Buscar número de expediente
        exp_match = re.search(r'expediente\s*n[°º]?\s*(\d+)[/\-](\d{2,4})', text, re.IGNORECASE)
        if exp_match:
            resolution_data['expediente_numero'] = exp_match.group(1)
            exp_year = exp_match.group(2)
            if len(exp_year) == 2:
                exp_year = "19" + exp_year if int(exp_year) > 50 else "20" + exp_year
            resolution_data['expediente_anio'] = exp_year
        
        return resolution_data

    def extract_convenio_data(self, text: str) -> Dict:
        """Extrae datos principales del convenio"""
        data = {
            'tipo_convenio': 'Marco',  # Valor por defecto
            'titulo': '',
            'duracion': '',
            'fecha_firma': '',
            'tipo_renovacion': '',
            'internacional': 'false',
            'objeto': '',
            'observaciones': ''
        }
        
        # Detectar tipo de convenio
        for tipo_key, tipo_value in self.tipos_convenio.items():
            if re.search(r'\b' + re.escape(tipo_key) + r'\b', text, re.IGNORECASE):
                data['tipo_convenio'] = tipo_value
                break
        
        # Extraer título (primera línea que parece título)
        title_match = re.search(r'convenio\s+(.+?)(?:\n|\.)', text, re.IGNORECASE)
        if title_match:
            data['titulo'] = title_match.group(1).strip()[:255]
        
        # Buscar duración
        duracion_patterns = [
            r'duraci[oó]n\s*:?\s*(\d+)\s*a[ñn]os?',
            r'vigencia\s*:?\s*(\d+)\s*a[ñn]os?',
            r'plazo\s*:?\s*(\d+)\s*a[ñn]os?'
        ]
        
        for pattern in duracion_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                data['duracion'] = match.group(1)
                break
        
        # Buscar fecha de firma
        firma_patterns = [
            r'firmado\s+el\s+(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})',
            r'suscr[i|í]pto?\s+el\s+(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})'
        ]
        
        for pattern in firma_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                if 'de' in pattern:
                    day, month_name, year = match.groups()
                    month_map = {
                        'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
                        'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
                        'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'
                    }
                    month = month_map.get(month_name.lower(), '01')
                    data['fecha_firma'] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                else:
                    year = match.group(3)
                    if len(year) == 2:
                        year = "19" + year if int(year) > 50 else "20" + year
                    data['fecha_firma'] = f"{year}-{match.group(2).zfill(2)}-{match.group(1).zfill(2)}"
                break
        
        # Detectar si es internacional
        if re.search(r'\b(internacional|extranjero|exterior)\b', text, re.IGNORECASE):
            data['internacional'] = 'true'
        
        # Buscar tipo de renovación
        for renov_key, renov_value in self.tipos_renovacion.items():
            if re.search(r'\b' + re.escape(renov_key) + r'\b', text, re.IGNORECASE):
                data['tipo_renovacion'] = renov_value
                break
        
        # Extraer objeto/propósito
        objeto_patterns = [
            r'objeto\s*:?\s*(.+?)(?:\n\n|\.\s*[A-Z])',
            r'prop[oó]sito\s*:?\s*(.+?)(?:\n\n|\.\s*[A-Z])',
            r'finalidad\s*:?\s*(.+?)(?:\n\n|\.\s*[A-Z])'
        ]
        
        for pattern in objeto_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                data['objeto'] = match.group(1).strip()[:500]
                break
        
        return data

    def extract_institutions(self, text: str) -> List[Dict]:
        """Extrae instituciones mencionadas en el texto"""
        institutions = []
        
        # Patrones para identificar instituciones
        institution_patterns = [
            r'(Universidad\s+[^,\n]+)',
            r'(Instituto\s+[^,\n]+)',
            r'(Ministerio\s+[^,\n]+)',
            r'(Gobierno\s+[^,\n]+)',
            r'(Provincia\s+de\s+\w+)',
            r'(Municipalidad\s+[^,\n]+)',
            r'(Fundaci[oó]n\s+[^,\n]+)',
            r'(Empresa\s+[^,\n]+)',
            r'(Asociaci[oó]n\s+[^,\n]+)'
        ]
        
        found_institutions = set()
        
        for pattern in institution_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                inst_name = match.group(1).strip()
                if len(inst_name) > 5 and inst_name.lower() not in found_institutions:
                    found_institutions.add(inst_name.lower())
                    
                    # Determinar tipo
                    inst_type = 'Universitaria'  # Por defecto
                    for tipo_key, tipo_value in self.tipos_institucion.items():
                        if tipo_key in inst_name.lower():
                            inst_type = tipo_value
                            break
                    
                    institutions.append({
                        'nombre': inst_name,
                        'tipo': inst_type,
                        'pais': 'Argentina',
                        'provincia': 'Salta',  # Asumir Salta por defecto
                        'localidad': 'Salta'
                    })
        
        return institutions[:5]  # Limitar a 5 instituciones

    def extract_signers(self, text: str) -> List[Dict]:
        """Extrae firmantes del texto"""
        signers = []
        
        # Patrones para nombres y cargos
        name_patterns = [
            r'([A-ZÁÉÍÓÚ][a-záéíóú]+)\s+([A-ZÁÉÍÓÚ][a-záéíóú]+(?:\s+[A-ZÁÉÍÓÚ][a-záéíóú]+)*)',
            r'Dr\.\s+([A-ZÁÉÍÓÚ][a-záéíóú]+)\s+([A-ZÁÉÍÓÚ][a-záéíóú]+)',
            r'Ing\.\s+([A-ZÁÉÍÓÚ][a-záéíóú]+)\s+([A-ZÁÉÍÓÚ][a-záéíóú]+)',
            r'Lic\.\s+([A-ZÁÉÍÓÚ][a-záéíóú]+)\s+([A-ZÁÉÍÓÚ][a-záéíóú]+)'
        ]
        
        found_names = set()
        
        for pattern in name_patterns:
            matches = re.finditer(pattern, text)
            for match in matches:
                nombre = match.group(1)
                apellido = match.group(2)
                full_name = f"{nombre} {apellido}".lower()
                
                if full_name not in found_names and len(nombre) > 2 and len(apellido) > 2:
                    found_names.add(full_name)
                    
                    # Buscar cargo cerca del nombre
                    cargo = 'Rector/a'  # Por defecto
                    context = text[max(0, match.start()-100):match.end()+100]
                    
                    for cargo_key, cargo_value in self.cargos.items():
                        if cargo_key in context.lower():
                            cargo = cargo_value
                            break
                    
                    signers.append({
                        'dni': '',  # No disponible en documentos antiguos
                        'nombre': nombre,
                        'apellido': apellido,
                        'cargo': cargo
                    })
        
        return signers[:5]  # Limitar a 5 firmantes

    def process_single_pdf(self, url: str) -> Dict:
        """Procesa un único PDF y retorna los datos extraídos"""
        filename = url.split('/')[-1]
        pdf_path = self.output_dir / filename
        
        # Descargar PDF
        if not self.download_pdf(url, filename):
            return None
        
        try:
            # Extraer texto
            text = self.extract_text_from_pdf(str(pdf_path))
            
            if len(text.strip()) < 50:
                logger.warning(f"Texto insuficiente en {filename}")
                return None
            
            # Extraer todos los datos
            resolution_data = self.extract_resolution_data(text, url)
            convenio_data = self.extract_convenio_data(text)
            institutions = self.extract_institutions(text)
            signers = self.extract_signers(text)
            
            # Formatear para CSV
            csv_row = {
                'resolucion': '|'.join([
                    resolution_data['numero'],
                    resolution_data['fecha'],
                    resolution_data['tipo'],
                    resolution_data['expediente_numero'],
                    resolution_data['expediente_anio'],
                    resolution_data['dependencia_id'],
                    resolution_data['link']
                ]),
                'tipo_convenio': convenio_data['tipo_convenio'],
                'titulo': convenio_data['titulo'],
                'duracion': convenio_data['duracion'],
                'fecha_firma': convenio_data['fecha_firma'],
                'tipo_renovacion': convenio_data['tipo_renovacion'],
                'internacional': convenio_data['internacional'],
                'objeto': convenio_data['objeto'],
                'observaciones': convenio_data['observaciones'],
                'instituciones': ';'.join([
                    f"{inst['nombre']}|{inst['tipo']}|{inst.get('cuit', '')}|{inst.get('web', '')}|{inst['pais']}|{inst['provincia']}|{inst['localidad']}"
                    for inst in institutions
                ]),
                'dependencias': 'Rectorado|UNSa|rectorado|Argentina|Salta|Salta|true',  # Por defecto
                'firmantes': ';'.join([
                    f"{signer['dni']}|{signer['nombre']}|{signer['apellido']}|{signer.get('email', '')}|Argentina|Salta|Salta|||{signer['cargo']}|true"
                    for signer in signers
                ])
            }
            
            # Limpiar PDF descargado para ahorrar espacio
            os.remove(pdf_path)
            
            return csv_row
            
        except Exception as e:
            logger.error(f"Error procesando {filename}: {e}")
            return None

    def process_urls_file(self, urls_file: str, output_csv: str = "convenios.csv"):
        """Procesa archivo con URLs y genera CSV"""
        try:
            with open(urls_file, 'r', encoding='utf-8') as f:
                urls = [line.strip() for line in f if line.strip()]
            
            logger.info(f"Procesando {len(urls)} URLs")
            
            # Cabeceras del CSV
            fieldnames = [
                'resolucion', 'tipo_convenio', 'titulo', 'duracion', 'fecha_firma',
                'tipo_renovacion', 'internacional', 'objeto', 'observaciones',
                'instituciones', 'dependencias', 'firmantes'
            ]
            
            output_path = self.output_dir / output_csv
            processed = 0
            errors = 0
            
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for i, url in enumerate(urls, 1):
                    logger.info(f"Procesando {i}/{len(urls)}: {url}")
                    
                    row_data = self.process_single_pdf(url)
                    if row_data:
                        writer.writerow(row_data)
                        processed += 1
                        logger.info(f"✓ Procesado exitosamente")
                    else:
                        errors += 1
                        logger.error(f"✗ Error procesando")
                    
                    # Pausa entre descargas
                    if i % 10 == 0:
                        logger.info(f"Pausa técnica... Procesados: {processed}, Errores: {errors}")
            
            logger.info(f"Proceso completado: {processed} exitosos, {errors} errores")
            logger.info(f"CSV generado: {output_path}")
            
        except Exception as e:
            logger.error(f"Error general: {e}")

def main():
    """Función principal"""
    print("Extractor de Convenios UNSa")
    print("=" * 50)
    
    # Verificar dependencias
    try:
        pytesseract.get_tesseract_version()
        print("✓ Tesseract OCR disponible")
    except:
        print("✗ Error: Tesseract OCR no encontrado")
        print("Instala Tesseract: https://github.com/tesseract-ocr/tesseract")
        return
    
    # Solicitar archivo de URLs
    urls_file = input("Archivo con URLs (ej: urls.txt): ").strip()
    if not os.path.exists(urls_file):
        print(f"Error: Archivo {urls_file} no existe")
        return
    
    # Crear extractor y procesar
    extractor = ConvenioExtractor()
    extractor.process_urls_file(urls_file)
    
    print("\n¡Proceso completado!")
    print(f"Revisa los logs en: convenio_extractor.log")
    print(f"CSV generado en: {extractor.output_dir}/convenios.csv")

if __name__ == "__main__":
    main()
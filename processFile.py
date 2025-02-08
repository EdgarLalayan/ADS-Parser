


import cv2
import numpy as np
import logging
from PIL import Image, ImageFilter, ExifTags,ImageEnhance
import os
import pytesseract
import re
import fitz
import cv2
from matplotlib import pyplot as plt
import numpy as np
import json
import boto3
import time
from azure.cognitiveservices.vision.computervision import ComputerVisionClient
from azure.cognitiveservices.vision.computervision.models import OperationStatusCodes
from msrest.authentication import CognitiveServicesCredentials
from pdf2image import convert_from_path




def correct_image_orientation(img):
    try:
        for orientation in ExifTags.TAGS.keys():
            if ExifTags.TAGS[orientation] == 'Orientation':
                break
        exif = dict(img._getexif().items())

        if exif[orientation] == 3:
            img = img.rotate(180, expand=True)
        elif exif[orientation] == 6:
            img = img.rotate(270, expand=True)
        elif exif[orientation] == 8:
            img = img.rotate(90, expand=True)
    except (AttributeError, KeyError, IndexError):
        # cases: image doesn't have getexif data or no orientation data
        pass
    return img

def imageProcessing(image_path,width_height=3, point_percent=0.40, MinFilter=3, config=None):
    # Update parameters from config if provided
    if config:
        width_height = config.get('width_height', width_height)
        point_percent = config.get('point_percent', point_percent)
        MinFilter = config.get('MinFilter', MinFilter)

    # Load the image from the specified path
    try:
        img = Image.open(image_path)
        img = correct_image_orientation(img)


    except IOError:
        logging.error("Unable to load image.")
        return None

    try:
        # Crop the image based on the specified dimensions
        original_width, original_height = img.size
        top = original_height * 0.10  # Top 10%
        bottom = original_height - (original_height * 0.20)  # Bottom 20%
        left = 10  # Left 20 pixels
        right = original_width - 10  # Right 20 pixels
        img = img.crop((left, top, right, bottom))
        # enhancer = ImageEnhance.Contrast(img)
        # img = enhancer.enhance(1.0)  # Increase contrast


        # Resize the image
        new_width = int(img.width * width_height)
        new_height = int(img.height * width_height)
        img = img.resize((new_width, new_height), Image.LANCZOS)

        # Convert to grayscale and apply adaptive thresholding
        img = img.convert('L')
        img = img.point(lambda p: 255 if p > img.getextrema()[1] * point_percent else 0, mode='1')

        # Apply sharpening and noise reduction
        img = img.filter(ImageFilter.SHARPEN)
        img = img.filter(ImageFilter.MinFilter(MinFilter))  # Erosion followed by dilation

        # Check if directory exists, create it if it doesn't
        image_dir = 'images'
        if not os.path.exists(image_dir):
            os.makedirs(image_dir)

        # Save the processed image
        img.save(f"{image_dir}/processed_image.png")
        print("Image saved as PNG.")
        
        return img

    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")
        return None

def extract_text_from_image(image, language='eng', tess_config='--oem 1 --psm 4'):
    if image is None:
        logging.warning("No image provided for text extraction.")
        return ""
    
    try:
        extracted_text = pytesseract.image_to_string(image, lang=language, config=tess_config)
        extracted_text = re.sub(r'(?<=[\w])\s*([-\u2013\u2014])\s*(?=[\w])', r'\1', extracted_text)

        return extracted_text
    except Exception as e:
        logging.error(f"Failed to extract text from image: {e}")
        return ""

def extract_text_with_aws(image_path):
    """
    Process an image to extract text using AWS Textract, save the output to a file,
    and return the extracted text.
    
    Parameters:
    - image_path: Path to the image file to be processed.
    - output_path: Path to save the extracted text output.
    
    Returns:
    - extracted_text: Extracted text from the image.
    """
    extracted_text = ""
    
    # Load AWS credentials and other settings from config.json
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
        aws_config = config['AWS']
    
    # Initialize a session using AWS credentials from the config file
    session = boto3.Session(
        aws_access_key_id=aws_config['AWS_ACCESS_KEY_ID'],  # Access key from config
        aws_secret_access_key=aws_config['AWS_SECRET_ACCESS_KEY'],  # Secret key from config
        region_name=aws_config['AWS_REGION']  # AWS region from config
    )
    
    # Create a Textract client
    textract = session.client('textract')
    
    # Load the image file
    with open(image_path, 'rb') as document:
        image_bytes = document.read()
    
    # Call Textract to process the image bytes
    response = textract.analyze_document(
        Document={'Bytes': image_bytes},
        FeatureTypes=["FORMS", "TABLES"]  # You can specify the features you want to analyze
    )
    
    # Extract text from the response
    for item in response['Blocks']:
        if item['BlockType'] == 'LINE':
            extracted_text += item['Text'] + '\n'
            print(item['Text'])  # Optionally print each line to the console as well
    
    # Save the detected text to a file
    with open('aws_output.txt', 'w') as text_file:
        text_file.write(extracted_text)
    
    return extracted_text

def extract_text_from_pdf_with_aws(pdf_path):
    """
    Process a PDF to extract text using AWS Textract asynchronously, save the output to a file,
    and return the extracted text.
    
    Parameters:
    - pdf_path: Path to the PDF file to be processed.
    - output_path: Path to save the extracted text output.
    
    Returns:
    - extracted_text: Extracted text from the PDF.
    """
    extracted_text = ""

    # Load AWS credentials and other settings from config.json
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
        aws_config = config['AWS']

    # Initialize a session using AWS credentials from the config file
    session = boto3.Session(
        aws_access_key_id=aws_config['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=aws_config['AWS_SECRET_ACCESS_KEY'],
        region_name=aws_config['AWS_REGION']
    )

    # Create a Textract client
    textract = session.client('textract')

    # Call Textract to process the PDF
    response = textract.start_document_text_detection(
        DocumentLocation={
            'S3Object': {
                'Bucket': 'your-bucket-name',
                'Name': pdf_path
            }
        }
    )
    
    job_id = response['JobId']
    print(f"Started job with ID: {job_id}")
    response = None

    while response is None or 'JobStatus' in response and response['JobStatus'] == 'IN_PROGRESS':
        print("Waiting for job to complete...")
        time.sleep(5)
        response = textract.get_document_text_detection(JobId=job_id)

    # Collecting text
    pages = []
    while response:
        pages.extend([item['Text'] for item in response['Blocks'] if item['BlockType'] == 'LINE'])
        if 'NextToken' in response:
            response = textract.get_document_text_detection(JobId=job_id, NextToken=response['NextToken'])
        else:
            response = None

    extracted_text = '\n'.join(pages)

    # Save the detected text to a file
    with open('extract_text_from_pdf_with_aws', 'w') as text_file:
        text_file.write(extracted_text)

    return extracted_text



def extract_text_with_azure(image_path):

    with open('config.json') as config_file:
        config = json.load(config_file)
        aws_config = config['AZURE']
        subscription_key = aws_config['AZURE_SUBSCRIPTION_KEY']
        endpoint = aws_config['AZURE_ENDPOINT']
    # Credentials setup

    # Authenticate the client
    credentials = CognitiveServicesCredentials(subscription_key=subscription_key)
    client = ComputerVisionClient(endpoint, credentials)
    with open(image_path, "rb") as image_stream:
        response = client.read_in_stream(image_stream, raw=True)
    operation_location = response.headers["Operation-Location"]
    operation_id = operation_location.split("/")[-1]

    while True:
        result = client.get_read_result(operation_id)
        if result.status not in [OperationStatusCodes.not_started, OperationStatusCodes.running]:
            break
    text = []
    if result.status == OperationStatusCodes.succeeded:
        for text_result in result.analyze_result.read_results:
            for line in text_result.lines:
                text.append(line.text)
    text =  "\n".join(text)
    with open('azure_output.txt', 'w') as text_file:
        text_file.write(text)

    return text 

def extract_text_with_azureBlocks(image_path):
    # Load configuration and authenticate the client
    with open('config.json') as f: 
        config = json.load(f)['AZURE']
    client = ComputerVisionClient(config['AZURE_ENDPOINT'], CognitiveServicesCredentials(config['AZURE_SUBSCRIPTION_KEY']))

    # Start the read operation
    with open(image_path, "rb") as image_stream:
        operation_location = client.read_in_stream(image_stream, raw=True).headers["Operation-Location"]
    operation_id = operation_location.split("/")[-1]

    # Wait for the read operation to complete
    result = client.get_read_result(operation_id)
    while result.status in [OperationStatusCodes.not_started, OperationStatusCodes.running]:
        result = client.get_read_result(operation_id)

    # Extract text if the operation succeeded
    if result.status == OperationStatusCodes.succeeded:
        text = '\n\n'.join([' '.join([line.text for line in text_result.lines]) for text_result in result.analyze_result.read_results])
    else:
        text = ""

    # Write output to a file
    with open('azure_output.txt', 'w') as f:
        f.write(text)
    
    return text

def save_text_to_file(text, filename="extracted_text.txt"):
    with open(filename, "w") as file:
        file.write(text)
    print("Text saved to file.")

def load_text_from_file(filename):

    try:
        with open(filename, "r") as file:
            text = file.read()
        print("Text successfully loaded from file.")
        return text
    except FileNotFoundError:
        print(f"Error: The file '{filename}' was not found.")
        return ""
    except Exception as e:
        print(f"An error occurred: {e}")
        return ""


def extract_text_from_pdf_with_fitz(pdf_path):
    """
    Extracts all text from a PDF file.
    
    :param pdf_path: The path to the PDF file to be processed.
    :return: The extracted text as a single string.
    """
    # Open the provided PDF file
    document = fitz.open(pdf_path)
    
    # Initialize a variable to store all the extracted text
    full_text = ""
    
    # Iterate through each page in the PDF
    for page in document:
        # Extract text from the page and add it to the full_text variable
        full_text += page.get_text()
    
    # Close the PDF after processing
    document.close()
    
    return full_text

def extract_text_from_pdf_with_fitz_Blocks(pdf_path):
    """
    Extracts all text from a PDF file with improved structure preservation.
    
    :param pdf_path: The path to the PDF file to be processed.
    :return: The extracted text as a single string, with improved grouping.
    """
    # Open the provided PDF file
    document = fitz.open(pdf_path)
    
    # Initialize a variable to store all the extracted text
    full_text = ""
    
    # Iterate through each page in the PDF
    for page in document:
        # Extract text block by block
        blocks = page.get_text("blocks")
        # Sort blocks by their position on the page (y0, x0)
        blocks.sort(key=lambda block: (block[1], block[0]))
        
        # Compile text from blocks
        page_text = ""
        for block in blocks:
            # Each block's text is at index 4
            page_text += block[4] + "=====\n"  # Append two newlines after each block
        
        # Add page text to the full document text
        full_text += page_text + "\n"  # Append an additional newline to separate pages
    
    # Close the PDF after processing
    document.close()
    
    return full_text



def pdf_to_images(pdf_file_path):
    # Преобразование PDF в изображения
    images = convert_from_path(pdf_file_path)
    return images

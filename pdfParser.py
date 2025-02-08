import json
import re
from datetime import datetime, timedelta
import base64
import io
import sys
import fitz
import os
import tempfile


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


def pdf_to_text(pdf_file):
    """
    Extract text from a PDF in a BytesIO stream by:
      1) Writing pdf_file (BytesIO) to a temp PDF file,
      2) Calling extract_text_from_pdf_with_fitz_Blocks(temp_path).
    """
    if not isinstance(pdf_file, io.BytesIO):
        raise ValueError("pdf_file must be a BytesIO object")

    # 1) Create a temporary PDF file
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(pdf_file.getvalue())  # Write in-memory PDF to disk
        tmp.flush()
        temp_path = tmp.name

    # 2) Call your existing function that expects a file path
    try:
        text = extract_text_from_pdf_with_fitz_Blocks(temp_path)
    finally:
        # 3) Clean up: remove the temp file from disk
        if os.path.exists(temp_path):
            os.remove(temp_path)

    return text


def process_text(in_text):

    # -----------------------------
    # STEP 1. Split into blocks
    # -----------------------------
    lines = in_text.splitlines()
    raw_chunks = []
    current = []
    for line in lines:
        if '=====' in line:
            if current:
                raw_chunks.append("\n".join(current))
                current = []
        else:
            current.append(line)
    if current:
        raw_chunks.append("\n".join(current))

    # Remove empty blocks (if any)
    raw_chunks = [c for c in raw_chunks if c.strip()]

    # Patterns
    pat_number_dash = re.compile(r'^\d+\s*-\s*')  # Example: "1 - Medical"
    pat_procedure = re.compile(r'(HIP INJECTION|LUMBAR EPIDURAL)', re.IGNORECASE)
    pat_digits_def = re.compile(r'^\d+-\d+$')  # Example: "56932-1"
    time_pattern = re.compile(r'\b(?:[01]?\d|2[0-3]):[0-5]\d(?:\s?[APap][Mm])?\b')  # hh:mm AM/PM or 24-hour

    final_blocks = []
    for block in raw_chunks:
        lines_in_block = block.splitlines()

        #
        # 2a. "Number-dash" (e.g., "1 - Medical") + preceding '*'
        #
        first_number_index = None
        for idx, ln in enumerate(lines_in_block):
            if pat_number_dash.search(ln):
                first_number_index = idx
                break
        if first_number_index is not None:
            group1 = lines_in_block[:first_number_index]
            group2 = lines_in_block[first_number_index:]
            if any(g1.strip().startswith('*') for g1 in group1):
                block = "\n".join(group1) + "\n=====\n" + "\n".join(group2)
                lines_in_block = block.splitlines()

        #
        # 2b. Procedure (HIP INJECTION / LUMBAR EPIDURAL) → if the next line starts with '*', insert '====='
        #
        new_lines = []
        for idx, ln in enumerate(lines_in_block):
            new_lines.append(ln)
            if (idx + 1) < len(lines_in_block):
                next_ln = lines_in_block[idx + 1]
                if pat_procedure.search(ln) and next_ln.strip().startswith('*'):
                    new_lines.append("=====")
        lines_in_block = new_lines

        #
        # 2c. Handle digit-dash pattern (e.g., "56932-1") → insert '=====' above it
        # 2d. Handle time_pattern → insert '=====' above it unless the next line is also time_pattern
        #
        final_lines = []
        for idx, ln in enumerate(lines_in_block):
            # Check the current line for "digit-dash" pattern
            if pat_digits_def.search(ln):
                # Ensure '=====' is not already above
                if final_lines and final_lines[-1].strip() != "=====":
                    final_lines.append("=====")

            # Check the current line for `time_pattern`
            if time_pattern.search(ln):
                # Ensure '=====' is added only if there is no time immediately above
                if final_lines:
                    last_line = final_lines[-1]
                    if not time_pattern.search(last_line) and last_line.strip() != "=====":
                        final_lines.append("=====")
            
            final_lines.append(ln)
        block = "\n".join(final_lines)

        if block.strip():
            final_blocks.append(block)

    # -----------------------------
    # STEP 3. Collect results
    # -----------------------------
    # Wrap each block by inserting "=====" before it
    result_lines = []
    for blk in final_blocks:
        result_lines.append("=====")
        result_lines.append(blk)

    return "\n".join(result_lines)


def extract_block(in_text):
    lines = in_text.splitlines()
    first_block = []
    found_first_delim = False

    while lines:
        line = lines.pop(0)
        # Check for the delimiter
        if '=====' in line:
            if found_first_delim:
                break
            else:
                found_first_delim = True
                continue

        # If we've passed the first delimiter, collect lines
        if found_first_delim:
            first_block.append(line)

    return "\n".join(first_block).strip()


def startParsingPDF(text, output_json_file=False):
    # print(text)
    or_pattern = r"OR ?\d+$|OR ?\d+(?=\s)"
    time_pattern = r'\b(?:[01]?\d|2[0-3]):[0-5]\d(?:\s?[APap][Mm])?\b'

    # Get company name
    company_name = get_company(text)

    patternNewLines = r"\n\s*\n"
    text = re.sub(patternNewLines, "\n=====\n", text)
    text = process_text(text)

    # Find OR sections and preprocess text
    or_sections = re.findall(or_pattern, text)
    text = text.split('\n')
    # text = [item for item in text if item and (item[0].isdigit() or item in ('F', 'M', 'AM', 'PM',) or 'OR' in item or 'PATIL' in item or '=====' in item)]

    # Initialize results
    results = {}
    current_or = None
    result = {
        'start_time': '',
        'end_time': '',
        'duration': '',
    }
    text = [t for t in text if all(x not in t for x in (
        'Page', 'Printed', '<image', 'Start', 'End', 'Dur.', 'Surgeon',
        'Procedure', 'Anes.', 'Allergies', 'Tags', 'MRN',
        'Age', 'Sex', 'Gender Identity'))]

    # Main processing loop
    while True:
        if len(text) <= 1:
            break
        txt = text[0]

        if 'OR' in txt.strip() or 'CANCELLED' in txt.strip():
            digits = ''.join(char for char in txt if char.isdigit())
            if digits:
                current_or = txt.strip()
                del text[0]

            elif txt.strip() == 'CANCELLED':
                current_or = txt.strip()
                or_sections.insert(0, current_or)

            elif txt.strip() == 'OR':
                current_or = 'OR ' + firstBlockArray[1].strip()
                del text[:2]

        if len(text) <= 1:
            break
        txt = text[0]
        # DETECT FIRST BLOCK
        if '=====' in txt and re.findall(time_pattern, text[1]):
            joinedtext = '\n'.join(text)
            firstBlock = extract_block(joinedtext)
            # print(firstBlock)
            firstBlockArray = firstBlock.split('\n')
            firstBlockArray = [element.strip() for element in firstBlockArray]
            del text[:len(firstBlockArray)+1]

            # print(firstBlockArray)

            while True:
                if len(firstBlockArray) == 0:
                    break
                txt = firstBlockArray[0]

                match_time = re.findall(time_pattern, txt)
                if match_time and txt.strip()[0].isdigit():
                    if result['start_time'] == '' and result['end_time'] == '' and result['duration'] == '':

                        if 'AM' in match_time[0] or 'PM' in match_time[0]:
                            result['start_time'] = match_time[0].split()[0]
                        else:
                            result['start_time'] = match_time[0]

                        del firstBlockArray[0]
                        # when 1Illinois(start end )
                        end_time = re.findall(time_pattern, firstBlockArray[0])
                        if end_time:
                            if 'AM' in end_time[0] or 'PM' in end_time[0]:
                                result['end_time'] = end_time[0].split()[0]
                            else:
                                result['end_time'] = end_time[0]
                            del firstBlockArray[0]

                            if firstBlockArray[0].isdigit():
                                result['duration'] = firstBlockArray[0]
                                del firstBlockArray[0]
                            if len(firstBlockArray) >= 2 or (len(firstBlockArray[0].split()) > 2) and all(isinstance(element, str) for element in firstBlockArray):

                                if len(firstBlockArray[0].split()) > 2:
                                    result['Surgeon'] = ' '.join(
                                        firstBlockArray[0].split()[:2])
                                    result['Procedure'] = firstBlockArray[0].replace(
                                        result['Surgeon'], '').strip()
                                elif len(firstBlockArray[0].split()) == 2 or len(firstBlockArray[0].split()) == 1:
                                    result['Surgeon'] = ''.join(
                                        firstBlockArray[0])
                                    del firstBlockArray[0]
                                    result['Procedure'] = ' '.join(
                                        firstBlockArray)
                                    if len(firstBlockArray[0].split()) < 2:
                                        result['Surgeon'] = result['Surgeon'] + ' ' + ''.join(
                                            firstBlockArray[0])
                                        del firstBlockArray[0]
                                        del result['Procedure']
                                        if len(firstBlockArray) > 0 and len(firstBlockArray[0].split()) != 1:
                                            result['Procedure'] = ' '.join(firstBlockArray).replace(
                                                result['Surgeon'], '').strip()
                                        elif len(firstBlockArray) > 0 and len(firstBlockArray[0].split()) == 1:
                                            result['Surgeon'] = result['Surgeon'] + ' ' + ''.join(
                                                firstBlockArray[0])
                                            del firstBlockArray[0]

                                    firstBlockArray = []

                                else:
                                    result['Surgeon'] = ' '.join(
                                        firstBlockArray)

                                firstBlockArray = []

                                if 'Procedure' not in result:
                                    procedure = extract_block('\n'.join(text))
                                    result['Procedure'] = procedure
                                    del text[:len(procedure.split('\n'))+1]

                                anes = extract_block('\n'.join(text))

                                result['Anes'] = anes
                                del text[:len(anes.split('\n'))+1]

                                tags = extract_block('\n'.join(text))
                                result['Tags'] = tags
                                del text[:len(tags.split('\n'))+1]

                                mrnAgeSex = extract_block('\n'.join(text))
                                mrnAgeSex = mrnAgeSex.split()

                                result['MRN'] = mrnAgeSex[0]
                                result['Age'] = mrnAgeSex[1]
                                result['Sex'] = mrnAgeSex[2]
                                del mrnAgeSex[:3]
                                del text[:4]

                                if len(mrnAgeSex) > 0:
                                    result['Gender Indentity'] = ' '.join(
                                        mrnAgeSex)
                                    del text[0]
                                predictProc = extract_block('\n'.join(text))
                                match_time = re.findall(
                                    time_pattern, predictProc.split('\n')[0])
                                if len(match_time) == 0 and 'OR' not in predictProc.split('\n')[0]:
                                    result['Procedure'] = result['Procedure'] + \
                                        '\n' + predictProc
                                    del text[:len(predictProc.split('\n'))+1]

                            if len(firstBlockArray) > 0 and all(isinstance(element, str) for element in firstBlockArray):
                                result['Surgeon'] = ' '.join(firstBlockArray)
                                procedure = extract_block('\n'.join(text))
                                result['Procedure'] = procedure
                                del text[:len(procedure.split('\n'))+1]

                            if current_or:
                                results.setdefault(
                                    current_or, []).append(result.copy())
                            else:
                                if or_sections:
                                    current_or = or_sections.pop(0)
                                    del or_sections[0]
                                    results.setdefault(
                                        current_or, []).append(result.copy())

                            result = {
                                'start_time': '',
                                'end_time': '',
                                'duration': '',
                            }
                            firstBlockArray = []
                            continue

                        else:
                            if firstBlockArray[0].isdigit():
                                result['Age'] = firstBlockArray[0]
                            elif firstBlockArray[1].isdigit():
                                result['Age'] = firstBlockArray[1]
                            elif 'mths' in firstBlockArray[0]:
                                result['Age'] = firstBlockArray[0]
                            else:
                                result['Age'] = ''

                        del firstBlockArray[0]
                        continue

                    if result['start_time'] != '' and result['end_time'] == '' and result['duration'] == '':
                        if 'AM' in match_time[0] or 'PM' in match_time[0]:
                            result['end_time'] = match_time[0].split()[0]
                        else:
                            result['end_time'] = match_time[0]

                        duration = firstBlockArray[1]
                        result['duration'] = duration

                        # Append the result to the current OR group
                        if current_or:
                            # Initialize the key in the results dictionary if it doesn't exist
                            if current_or not in results:
                                # Add an empty list for the new OR key
                                results[current_or] = []
                            results[current_or].append(result.copy())

                        result = {
                            'start_time': '',
                            'end_time': '',
                            'duration': '',
                        }
                        del firstBlockArray[0]
                        continue

                if 'F' == txt or 'M' == txt and result['start_time'] != '':
                    result['sex'] = txt
                    result['duration'] = firstBlockArray[1]
                    result['Perf. Physician'] = firstBlockArray[2]
                    result['Anes'] = firstBlockArray[3]

                    if result['start_time'] != '' and result['duration'] != '':
                        start_time = datetime.strptime(
                            result['start_time'], '%H:%M')
                        duration = int(result['duration'])
                        end_time = start_time + timedelta(minutes=duration)
                        result['end_time'] = end_time.strftime('%H:%M')

                        if current_or:
                            procedure = extract_block('\n'.join(text))
                            result['Procedure'] = procedure
                            del text[:len(procedure.split('\n'))+1]
                            if current_or not in results:
                                results[current_or] = []
                            results[current_or].append(result.copy())

                        result = {
                            'start_time': '',
                            'end_time': '',
                            'duration': '',
                        }

                    del firstBlockArray[:4]
                    continue

                # If the line matches an OR section, start a new group
                # if re.match(or_pattern, txt):
                #     current_or = txt.strip()
                #     if current_or not in results:
                #         results[current_or] = []
                #     del firstBlockArray[0]
                #     continue

                if 'OR' in txt.strip() or 'CANCELLED' in txt.strip():
                    digits = ''.join(char for char in txt if char.isdigit())
                    if digits:
                        current_or = txt.strip()
                        del firstBlockArray[0]
                    elif txt.strip() == 'OR':
                        current_or = 'OR ' + firstBlockArray[1].strip()
                        del firstBlockArray[:2]
                    elif txt.strip() == 'CANCELLED':
                        current_or = txt.strip()
                    else:
                        del firstBlockArray[0]
                        continue

                    if current_or not in results:
                        results[current_or] = []
                    continue

                del firstBlockArray[0]

        else:

            del text[0]

    # Prepare final output
    final_output = {
        'company': company_name,
        'or_sections': results
    }

    # Save results to JSON
    if output_json_file:
        with open(output_json_file, 'w') as json_file:
            json.dump(final_output, json_file, indent=4)

    return final_output


def natural_sort_key(key):
    """
    Generate a natural sorting key to sort files in the way humans expect.
    Splits strings into a list of integers and strings for natural ordering.
    """
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', key)]


def get_company(text):
    # List of companies to check
    companies = ['Illinois Sports Medicine & Orthopedic Surgery CTR',
                 'Golf Surgical Center', 'Hawthorn Surgery Center']
    text = text.lower().strip()
    for company in companies:
        if company.lower().strip() in text:
            return company
    return None


def calculate_time_fields(entry):
    """
    Calculate duration or end_time based on given start_time, end_time, and duration.
    Returns time in 24-hour format.
    """
    def add_am_pm(time_str):
        # If no AM/PM is provided, default to AM
        if "AM" not in time_str.upper() and "PM" not in time_str.upper():
            return f"{time_str} AM"
        return time_str

    if 'start_time' in entry and 'duration' in entry and entry['start_time'] and entry['duration']:
        # Ensure start time has AM/PM
        start_time = datetime.strptime(
            add_am_pm(entry['start_time']), '%I:%M %p')
        duration = int(entry['duration'])
        end_time = start_time + timedelta(minutes=duration)
        return end_time.strftime('%H:%M')  # Return end_time in 24-hour format

    return None  # Return None if the required fields are not available





if __name__ == "__main__":
    try:
        pdf_b64 = sys.argv[1]

        pdf_data = base64.b64decode(pdf_b64)

        pdf_stream = io.BytesIO(pdf_data)

        text = pdf_to_text(pdf_stream)
        result = startParsingPDF(text)
        # 5) Output the extracted text as JSON
        print(json.dumps({
            "status": "success",
            "data": result
        }))
    except Exception as e:
        print(json.dumps({
            "status": "error",
            "message": str(e)
        }))

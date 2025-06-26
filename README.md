# HPV Vaccination ETL

This git repository contains code to process HPV Vaccination coverage data.

The code performs a basic ETL process on the data available and uploads the data to the NCL Data Warehouse

## Steps to Run This ETL Project

For a detailed overview of the First Time Installation, check the NCL scripting onboarding document [here](https://nhs.sharepoint.com/:w:/r/sites/msteams_38dd8f/Shared%20Documents/Document%20Library/Documents/Git%20Integration/Internal%20Scripting%20Guide.docx?d=wc124f806fcd8401b8d8e051ce9daab87&csf=1&web=1&e=CmK9V3).

An overview of the setup is as follows:

1. **Set Up Your Environment**  
   - Install Python
   - Install [Visual Studio Code](https://code.visualstudio.com/)  
   - Install the Python extension in VS Code  

2. **Clone This Repository**  
   - Open VS Code  
   - Press `Ctrl+Shift+P` and select `Git: Clone`  
   - Paste this repo’s URL and choose a folder to save the project

3. **Create a Virtual Environment**
   ```bash
   python -m venv venv
   ```

4. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

**DOWNLOADING THE DATA:**

The datasets for this project can be found [here](https://www.gov.uk/government/collections/vaccine-uptake).

Use Ctrl F to search for 'HPV' to get to the correct section. There you will find links to different academic years. This Project uses data back until 2019.

For each year do the following:
- Click the link e.g. "Human papillomavirus (HPV) vaccine coverage estimates in England: 2023 to 2024"
- Click the link which contains the Excel file (usually second one down), Download and save in the 'Data' folder in the VS code project directory.


The tab used in the ETL process is called 'Local_authority'. 

Currently, all Excel files must be formatted in the same way for the code to run accurately. The format is the same as in the 2023/2024 file. The following are the conditions that the format needs to meet:

- The text in Cell A1 must end with the date in this format 'September 20XX to August 20XX'. This is because the code takes the date from the end of this cell and creates a column using it.
- Column Headers must begin at A3
- The first row of data must begin at A4. 
- Any rows additional rows can be deleted, such as 'Range'

Once Local_authority tab is formatted this way, the files can be saved again in the 'Data' folder of the project directory.


**EXCECUTING THE CODE:**

- Open the project directory.
- Open VS Code.
- Open a new folder (Ctrl+K Ctrl+O) and select the HPV_DATA folder .
- Enable the virtual environment (see the onboarding document linked in the First Time Installation section).
- Execute the src/main.py file by opening the src/main.py file in VSCode and using the Run arrow button in the top right of the window.
- Once Excecuted, the code will print 'Uploading new data (*number of rows*)'

## Licence
This repository is dual licensed under the Open Government v3 & MIT. All code can outputs are subject to Crown Copyright.

## Contact
Eric Pinto - eric.pinto@nhs.net
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from slack_sdk import WebClient
import logging
from SlackBot.Static.Constants import *
from datetime import datetime, time
from zoneinfo import ZoneInfo
import os 
import pickle
import requests
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

logger = logging.getLogger(__name__)

scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file(r"SlackBot\External\Credentials.json", scopes=scopes)
client = gspread.authorize(creds)

class Initialization():
    def grab_impvol(self, external_impvol):
        logger.debug(f" Startup | grab_impvol | Note: Running")
        
        output_impvol = {}

        for task in external_impvol:
            workbook = client.open_by_key(task["sheet_id"])
            sheet = workbook.worksheet(task["sheet_name"])
            cell_value = sheet.cell(task["row_number"], task["col_number"]).value
            
            logger.debug(f" Startup | grab_impvol | Sheet: {task['sheet_name']} | Row: {task['row_number']}  | Column: {task['col_number']}")
            
            if "ES" in task["sheet_name"]:
                output_impvol['es_impvol'] = cell_value
            elif "NQ" in task["sheet_name"]:
                output_impvol['nq_impvol'] = cell_value
            elif "RTY" in task["sheet_name"]:
                output_impvol['rty_impvol'] = cell_value
            elif "CL" in task["sheet_name"]:
                output_impvol['cl_impvol'] = cell_value
                
        es_impvol = float(output_impvol['es_impvol'].strip('%'))
        nq_impvol = float(output_impvol['nq_impvol'].strip('%'))
        rty_impvol = float(output_impvol['rty_impvol'].strip('%'))
        cl_impvol = float(output_impvol['cl_impvol'].strip('%'))
        
        logger.debug(f" Startup | grab_impvol | ES_IMP: {es_impvol} | NQ_IMP: {nq_impvol} | RTY_IMP: {rty_impvol} | CL_IMP: {cl_impvol}")
        return es_impvol, nq_impvol, rty_impvol, cl_impvol
    
    def grab_bias(self, external_bias):
        logger.debug(f" Startup | grab_impvol | Note: Running")
        
        output_bias = {}

        for task in external_bias:
            workbook = client.open_by_key(task["sheet_id"])
            sheet = workbook.worksheet(task["sheet_name"])
            cell_value = sheet.cell(task["row_number"], task["col_number"]).value
            
            logger.debug(f" Startup | grab_bias | Sheet: {task['sheet_name']} | Row: {task['row_number']}  | Column: {task['col_number']}")
            
            if "ES" in task["sheet_name"]:
                output_bias['es_bias'] = cell_value
            elif "NQ" in task["sheet_name"]:
                output_bias['nq_bias'] = cell_value
            elif "RTY" in task["sheet_name"]:
                output_bias['rty_bias'] = cell_value
            elif "CL" in task["sheet_name"]:
                output_bias['cl_bias'] = cell_value
                
        es_bias = output_bias['es_bias']
        nq_bias = output_bias['nq_bias']
        rty_bias = output_bias['rty_bias']
        cl_bias = output_bias['cl_bias']
 
        logger.debug(f" Startup | grab_bias | ES_Bias: {es_bias} | NQ_Bias: {nq_bias} | RTY_Bias: {rty_bias} | CL_Bias: {cl_bias}")
        return es_bias, nq_bias, rty_bias, cl_bias
    
    def publish_prep(self):
        
        logger.info("Startup | publish_prep | Starting publish_prep function.")
        
        slack_token = os.getenv('SLACK_TOKEN') 
        if not slack_token:
            logger.error(" Startup | publish_prep | Note: Slack Token Non existent")
            
        client = WebClient(token=slack_token)
        
        preps = {
            'ES': {
                'drive_folder_id': '1FjvgLwW4I7lLZmv33IeDcsL1X2urOOSy', 
                'slack_channel': 'C07RPPLTPKN',  
                'icon': ':large_blue_square:',
            },
            'NQ': {
                'drive_folder_id': '10RPtoqSu5okb2g8628avMFj6dNhJztEi', 
                'slack_channel': 'C07RM6R9N4B',
                'icon': ':large_green_square:',
            },
            'RTY': {
                'drive_folder_id': '1PHlBBlnawCpkzEAr_L3Rg5m4wFnpEeyJ', 
                'slack_channel': 'C07RJDL05A9',  
                'icon': ':large_orange_square:',
            },
            'CL': {
                'drive_folder_id': '164e7-8yvec_EoAdG0T4Px36xL2sFxAcz', 
                'slack_channel': 'C07S001K5GR',  
                'icon': ':large_purple_square:',
            }
        }
        
        try:
            creds = Credentials.from_service_account_file(
                r"SlackBot\External\Credentials.json",
                scopes=['https://www.googleapis.com/auth/drive.readonly']
            )
            drive_service = build('drive', 'v3', credentials=creds)
            logger.info("Startup | publish_prep | Google Drive API client initialized.")
        except Exception as e:
            logger.error(f"Startup | publish_prep | Failed to initialize Google Drive API client: {e}")
            return
        
        for product, config in preps.items():
            drive_folder_id = config['drive_folder_id']
            slack_channel = config['slack_channel']
            icon = config['icon']
            date_str = datetime.now().strftime('%m-%d-%Y')
            filename = f"{product}_PREP_{date_str}.pdf"
            
            logger.info(f"Startup | publish_prep | Processing {product} prep.")
            logger.debug(f"Startup | publish_prep | Expected filename: {filename}")
            
            query = f"name='{filename}' and '{drive_folder_id}' in parents and trashed=false"
            try:
                results = drive_service.files().list(
                    q=query,
                    spaces='drive',
                    fields='files(id, name)',
                    pageSize=1
                ).execute()
                files = results.get('files', [])
                
                if not files:
                    logger.error(f"Startup | publish_prep | File {filename} not found in Drive folder {drive_folder_id}.")
                    continue
                
                file_id = files[0]['id']
                logger.info(f"Startup | publish_prep | Found file {filename} with ID {file_id}.")
            except Exception as e:
                logger.error(f"Startup | publish_prep | Error searching for file {filename}: {e}")
                continue

            try:
                request = drive_service.files().get_media(fileId=file_id)
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    if status:
                        logger.debug(f"Startup | publish_prep | Downloading {filename}: {int(status.progress() * 100)}%.")
                fh.seek(0)
                with open(filename, 'wb') as f:
                    f.write(fh.read())
                logger.info(f"Startup | publish_prep | Downloaded {filename} successfully.")
            except Exception as e:
                logger.error(f"Startup | publish_prep | Error downloading file {filename}: {e}")
                continue
            
            try:
                upload_file = client.files_upload_v2(
                    channel= f"{slack_channel}",
                    title= f"{product} Prep",
                    file= f"{filename}",
                    initial_comment= f"{icon} *{product} Prep For {date_str}* {icon}",
                )
            except Exception as e:
                logger.error(f"Failed to upload {product} prep to {slack_channel}")
            
            try:
                os.remove(filename)
                logger.info(f"Startup | publish_prep | Deleted local file {filename}.")
            except Exception as e:
                logger.warning(f"Startup | publish_prep | Could not delete local file {filename}: {e}")
        
        logger.info("Startup | publish_prep | Completed publish_prep function.")

    def prep_data(files):
        all_variables = {}
        
        period_equity = {
            'A': time(9, 30),
            'B': time(10, 0),
            'C': time(10, 30),
            'D': time(11, 0),
            'E': time(11, 30),
            'F': time(12, 0),
            'G': time(12, 30),
            'H': time(13, 0),
            'I': time(13, 30),
            'J': time(14, 0),
            'K': time(14, 30),
            'L': time(15, 0),
            'M': time(15, 30),                                                                                                                        
        }
        period_crude = {
            'A': time(9, 0),
            'B': time(9, 30),
            'C': time(10, 0),
            'D': time(10, 30),
            'E': time(11, 0),
            'F': time(11, 30),
            'G': time(12, 0),
            'H': time(12, 30),
            'I': time(13, 0),
            'J': time(13, 30),
            'K': time(14, 0),        
        }        
        
        for task in files:  
            
            est = ZoneInfo('America/New_York')
            now = datetime.now(est).time()
            process_task = False
            
            if "CL" in task["name"]:
                start_time = time(9, 0)
                end_time = time(14, 30)
                if start_time <= now <= end_time:
                    process_task = True 
                    logger.debug(f" Startup | prep_data | Task: {task["name"]} | Process: {process_task}")
            elif any(name in task["name"] for name in ["ES", "NQ", "RTY"]):
                start_time = time(9, 30)
                end_time = time(16, 0)
                if start_time <= now <= end_time:
                    process_task = True 
                    logger.debug(f" Startup | prep_data | Task: {task["name"]} | Process: {process_task}")
            else:
                process_task = False
                logger.debug(f" Startup | prep_data | Task: {task["name"]} | Process: {process_task} | Note: Out of Time Range For Product")
                
            if not process_task:
                continue
            
            if task["header_row"] == 1:
                data = pd.read_csv(task["filepath"], delimiter='\t', header=None)
            elif task["header_row"] == 0:
                data = pd.read_csv(task["filepath"], delimiter='\t')
                data = data.reset_index()
            else:
                raise ValueError("header_row should be either 0 or 1")
            
            # Set Header and Configure DF
            pd.options.mode.copy_on_write = True
            data.columns = data.iloc[task["iloc1"]]
            data = data[task["iloc2"]:]
            
            # Drop Unwanted Columns and N/A
            existing_columns_to_drop = [col for col in columns_to_drop if col in data.columns]
            data = data.drop(columns=existing_columns_to_drop)
            df_cleaned = data.loc[:, data.columns.notna()]
            data = df_cleaned
            data = data.dropna()
            
            # Set Date-Time As Index
            if 'Date Time' in data.columns:
                data['Date Time'] = pd.to_datetime(data['Date Time'], errors='coerce')
                data.set_index('Date Time', inplace=True)
              
            # Converting Columns to float
            for columns in task["columns"]:
                data[columns] = data[columns].str.replace(',', '.').astype(float)
                
            logger.debug(f" Startup | prep_data | Task: {task["name"]} | Data-Frame: \n{data.head()}")
            variables = {}
            
            match task["name"]:
                case "ES_1":
                    # ------------------- Use Integer Based iLoc ----------------------- #
                    variables['ES_DAY_OPEN'] = float(data.iloc[0]['[ID2.SG1] Day_Open'])
                    variables['ES_DAY_HIGH'] = float(data.iloc[0]['[ID2.SG2] Day_High'])
                    variables['ES_DAY_LOW'] = float(data.iloc[0]['[ID2.SG3] Day_Low'])
                    variables['ES_DAY_CLOSE'] = float(data.iloc[0]['[ID2.SG4] Day_Close'])
                    variables['ES_DAY_VPOC'] = float(data.iloc[0]['[ID1.SG1] Day_Vpoc'])
                    variables['ES_PRIOR_VPOC'] = float(data.iloc[0]['[ID9.SG1] Prior_Vpoc'])
                    variables['ES_PRIOR_HIGH'] = float(data.iloc[0]['[ID8.SG2] Prior_High'])
                    variables['ES_PRIOR_LOW'] = float(data.iloc[0]['[ID8.SG3] Prior_Low'])
                    variables['ES_PRIOR_CLOSE'] = float(data.iloc[0]['[ID8.SG4] Prior_Close'])
                    variables['ES_RVOL'] = float(data.iloc[0]['[ID6.SG1] R_Vol'])
                    variables['ES_CUMULATIVE_RVOL'] = float(data.iloc[0]['[ID6.SG2] R_Vol_Cumulative'])
                    variables['ES_TOTAL_RTH_DELTA'] = float(data.iloc[0]['[ID4.SG4] Total_Delta'])
                    # ---------------------- Use Date Date Time Loc ------------------------- #
                    latest_date = data.index.max().date()
                    period_datetimes = {}

                    for period_label, period_time in period_equity.items():
                        specific_datetime = datetime.combine(latest_date, period_time)
                        period_datetimes[period_label] = specific_datetime

                    for period_label, specific_datetime in period_datetimes.items():    
                        if specific_datetime in data.index:
                            match period_label:
                                case 'A':
                                    variables['ES_A_HIGH'] = float(data.loc[specific_datetime]['[ID10.SG1] A_High'])
                                    variables['ES_A_LOW'] = float(data.loc[specific_datetime]['[ID10.SG2] A_Low'])    
                                case 'B':
                                    variables['ES_B_HIGH'] = float(data.loc[specific_datetime]['[ID11.SG1] B_High'])
                                    variables['ES_B_LOW'] = float(data.loc[specific_datetime]['[ID11.SG2] B_Low'])
                                case 'C':
                                    variables['ES_C_HIGH'] = float(data.loc[specific_datetime]['[ID12.SG1] C_High'])
                                    variables['ES_C_LOW'] = float(data.loc[specific_datetime]['[ID12.SG2] C_Low'])
                                case 'D':
                                    variables['ES_D_HIGH'] = float(data.loc[specific_datetime]['[ID13.SG1] D_High'])
                                    variables['ES_D_LOW'] = float(data.loc[specific_datetime]['[ID13.SG2] D_Low'])
                                case 'E':
                                    variables['ES_E_HIGH'] = float(data.loc[specific_datetime]['[ID14.SG1] E_High'])
                                    variables['ES_E_LOW'] = float(data.loc[specific_datetime]['[ID14.SG2] E_Low'])
                                case 'F':
                                    variables['ES_F_HIGH'] = float(data.loc[specific_datetime]['[ID15.SG1] F_High'])
                                    variables['ES_F_LOW'] = float(data.loc[specific_datetime]['[ID15.SG2] F_Low'])
                                case 'G':
                                    variables['ES_G_HIGH'] = float(data.loc[specific_datetime]['[ID16.SG1] G_High'])
                                    variables['ES_G_LOW'] = float(data.loc[specific_datetime]['[ID16.SG2] G_Low'])
                                case 'H':
                                    variables['ES_H_HIGH'] = float(data.loc[specific_datetime]['[ID17.SG1] H_High'])
                                    variables['ES_H_LOW'] = float(data.loc[specific_datetime]['[ID17.SG2] H_Low'])
                                case 'I':
                                    variables['ES_I_HIGH'] = float(data.loc[specific_datetime]['[ID18.SG1] I_High'])
                                    variables['ES_I_LOW'] = float(data.loc[specific_datetime]['[ID18.SG2] I_Low'])
                                case 'J':
                                    variables['ES_J_HIGH'] = float(data.loc[specific_datetime]['[ID19.SG1] J_High'])
                                    variables['ES_J_LOW'] = float(data.loc[specific_datetime]['[ID19.SG2] J_Low'])
                                case 'K':
                                    variables['ES_K_HIGH'] = float(data.loc[specific_datetime]['[ID20.SG1] K_High'])
                                    variables['ES_K_LOW'] = float(data.loc[specific_datetime]['[ID20.SG2] K_Low'])
                                case 'L':
                                    variables['ES_L_HIGH'] = float(data.loc[specific_datetime]['[ID21.SG1] L_High'])
                                    variables['ES_L_LOW'] = float(data.loc[specific_datetime]['[ID21.SG2] L_Low'])
                                case 'M':
                                    variables['ES_M_HIGH'] = float(data.loc[specific_datetime]['[ID22.SG1] M_High'])
                                    variables['ES_M_LOW'] = float(data.loc[specific_datetime]['[ID22.SG2] M_Low'])
                        else:
                            pass
                case "ES_2":
                    # ------------------- Use Integer Based Loc ----------------------- #
                    variables['ES_ETH_VWAP'] = float(data.iloc[0]['[ID7.SG1] ETH_VWAP'])
                    variables['ES_ETH_VWAP_P2'] = float(data.iloc[2]['[ID7.SG1] ETH_VWAP'])
                    variables['ES_ETH_TOP_1'] = float(data.iloc[0]['[ID7.SG2] Top_1'])
                    variables['ES_ETH_BOTTOM_1'] = float(data.iloc[0]['[ID7.SG3] Bottom_1'])
                    variables['ES_ETH_TOP_2'] = float(data.iloc[0]['[ID7.SG4] Top_2'])
                    variables['ES_ETH_BOTTOM_2'] = float(data.iloc[0]['[ID7.SG5] Bottom_2'])
                    variables['ES_CPL'] = float(data.iloc[0]['[ID2.SG1] CPL'])
                    variables['ES_5D_VPOC'] = float(data.iloc[0]['[ID4.SG2] 5DVPOC'])
                    variables['ES_20D_VPOC'] = float(data.iloc[0]['[ID3.SG2] 20DVPOC'])
                    variables['ES_P_WOPEN'] = float(data.iloc[0]['[ID5.SG1] P_WOPEN'])
                    variables['ES_P_WHIGH'] = float(data.iloc[0]['[ID5.SG2] P_WHIGH'])
                    variables['ES_P_WLO'] = float(data.iloc[0]['[ID5.SG3] P_WLO'])
                    variables['ES_P_WCLOSE'] = float(data.iloc[0]['[ID5.SG4] P_WCLOSE'])
                    variables['ES_P_WVPOC'] = float(data.iloc[0]['[ID11.SG1] P_WVPOC'])
                    variables['ES_WVWAP'] = float(data.iloc[0]['[ID10.SG1] WVWAP'])
                    variables['ES_P_MOPEN'] = float(data.iloc[0]['[ID8.SG1] P_MOPEN'])
                    variables['ES_P_MHIGH'] = float(data.iloc[0]['[ID8.SG2] P_MHIGH'])
                    variables['ES_P_MLO'] = float(data.iloc[0]['[ID8.SG3] P_MLO'])
                    variables['ES_P_MCLOSE'] = float(data.iloc[0]['[ID8.SG4] P_MCLOSE'])
                    variables['ES_P_MVPOC'] = float(data.iloc[0]['[ID12.SG1] P_MVPOC'])
                    variables['ES_MVWAP'] = float(data.iloc[0]['[ID1.SG1] MVWAP'])
                case "ES_3":
                    variables['ES_IB_ATR'] = float(data.iloc[1]['[ID2.SG1] IB ATR'])
                    variables['ES_IB_HIGH'] = float(data.iloc[0]['[ID1.SG2] IBH'])
                    variables['ES_IB_LOW'] = float(data.iloc[0]['[ID1.SG3] IBL'])
                    variables['ES_PRIOR_IB_HIGH'] = float(data.iloc[1]['[ID1.SG2] IBH'])
                    variables['ES_PRIOR_IB_LOW'] = float(data.iloc[1]['[ID1.SG3] IBL'])                    
                case "ES_4":
                    variables['ES_OVNH'] = float(data.iloc[0]['[ID1.SG2] OVN H'])
                    variables['ES_OVNL'] = float(data.iloc[0]['[ID1.SG3] OVN L'])
                    variables['ES_TOTAL_OVN_DELTA'] = float(data.iloc[0]['[ID3.SG4] OVN Total'])
                case "ES_5":
                    variables['ES_OVNTOIB_HI'] = float(data.iloc[0]['[ID1.SG2] OVNTOIB_HI'])
                    variables['ES_OVNTOIB_LO'] = float(data.iloc[0]['[ID1.SG3] OVNTOIB_LO'])
                case "ES_6":
                    variables['ES_EURO_IBH'] = float(data.iloc[0]['[ID1.SG2] EURO IBH'])
                    variables['ES_EURO_IBL'] = float(data.iloc[0]['[ID1.SG3] EURO IBL'])
                case "ES_7":
                    variables['ES_ORH'] = float(data.iloc[0]['[ID1.SG2] ORH'])
                    variables['ES_ORL'] = float(data.iloc[0]['[ID1.SG3] ORL'])
                case "NQ_1":
                    # ------------------- Use Integer Based iLoc ----------------------- #
                    variables['NQ_DAY_OPEN'] = float(data.iloc[0]['[ID2.SG1] Day_Open'])
                    variables['NQ_DAY_HIGH'] = float(data.iloc[0]['[ID2.SG2] Day_High'])
                    variables['NQ_DAY_LOW'] = float(data.iloc[0]['[ID2.SG3] Day_Low'])
                    variables['NQ_DAY_CLOSE'] = float(data.iloc[0]['[ID2.SG4] Day_Close'])
                    variables['NQ_DAY_VPOC'] = float(data.iloc[0]['[ID1.SG1] Day_Vpoc'])
                    variables['NQ_PRIOR_VPOC'] = float(data.iloc[0]['[ID9.SG1] Prior_Vpoc'])
                    variables['NQ_PRIOR_HIGH'] = float(data.iloc[0]['[ID8.SG2] Prior_High'])
                    variables['NQ_PRIOR_LOW'] = float(data.iloc[0]['[ID8.SG3] Prior_Low'])
                    variables['NQ_PRIOR_CLOSE'] = float(data.iloc[0]['[ID8.SG4] Prior_Close'])
                    variables['NQ_RVOL'] = float(data.iloc[0]['[ID6.SG1] R_Vol'])
                    variables['NQ_CUMULATIVE_RVOL'] = float(data.iloc[0]['[ID6.SG2] R_Vol_Cumulative'])
                    variables['NQ_TOTAL_RTH_DELTA'] = float(data.iloc[0]['[ID4.SG4] Total_Delta'])
                    # ---------------------- Use Date Date Time Loc ------------------------- #
                    latest_date = data.index.max().date()
                    period_datetimes = {}

                    for period_label, period_time in period_equity.items():
                        specific_datetime = datetime.combine(latest_date, period_time)
                        period_datetimes[period_label] = specific_datetime

                    for period_label, specific_datetime in period_datetimes.items():    
                        if specific_datetime in data.index:
                            match period_label:
                                case 'A':
                                    variables['NQ_A_HIGH'] = float(data.loc[specific_datetime]['[ID10.SG1] A_High'])
                                    variables['NQ_A_LOW'] = float(data.loc[specific_datetime]['[ID10.SG2] A_Low'])    
                                case 'B':
                                    variables['NQ_B_HIGH'] = float(data.loc[specific_datetime]['[ID11.SG1] B_High'])
                                    variables['NQ_B_LOW'] = float(data.loc[specific_datetime]['[ID11.SG2] B_Low'])
                                case 'C':
                                    variables['NQ_C_HIGH'] = float(data.loc[specific_datetime]['[ID12.SG1] C_High'])
                                    variables['NQ_C_LOW'] = float(data.loc[specific_datetime]['[ID12.SG2] C_Low'])
                                case 'D':
                                    variables['NQ_D_HIGH'] = float(data.loc[specific_datetime]['[ID13.SG1] D_High'])
                                    variables['NQ_D_LOW'] = float(data.loc[specific_datetime]['[ID13.SG2] D_Low'])
                                case 'E':
                                    variables['NQ_E_HIGH'] = float(data.loc[specific_datetime]['[ID14.SG1] E_High'])
                                    variables['NQ_E_LOW'] = float(data.loc[specific_datetime]['[ID14.SG2] E_Low'])
                                case 'F':
                                    variables['NQ_F_HIGH'] = float(data.loc[specific_datetime]['[ID15.SG1] F_High'])
                                    variables['NQ_F_LOW'] = float(data.loc[specific_datetime]['[ID15.SG2] F_Low'])
                                case 'G':
                                    variables['NQ_G_HIGH'] = float(data.loc[specific_datetime]['[ID16.SG1] G_High'])
                                    variables['NQ_G_LOW'] = float(data.loc[specific_datetime]['[ID16.SG2] G_Low'])
                                case 'H':
                                    variables['NQ_H_HIGH'] = float(data.loc[specific_datetime]['[ID17.SG1] H_High'])
                                    variables['NQ_H_LOW'] = float(data.loc[specific_datetime]['[ID17.SG2] H_Low'])
                                case 'I':
                                    variables['NQ_I_HIGH'] = float(data.loc[specific_datetime]['[ID18.SG1] I_High'])
                                    variables['NQ_I_LOW'] = float(data.loc[specific_datetime]['[ID18.SG2] I_Low'])
                                case 'J':
                                    variables['NQ_J_HIGH'] = float(data.loc[specific_datetime]['[ID19.SG1] J_High'])
                                    variables['NQ_J_LOW'] = float(data.loc[specific_datetime]['[ID19.SG2] J_Low'])
                                case 'K':
                                    variables['NQ_K_HIGH'] = float(data.loc[specific_datetime]['[ID20.SG1] K_High'])
                                    variables['NQ_K_LOW'] = float(data.loc[specific_datetime]['[ID20.SG2] K_Low'])
                                case 'L':
                                    variables['NQ_L_HIGH'] = float(data.loc[specific_datetime]['[ID21.SG1] L_High'])
                                    variables['NQ_L_LOW'] = float(data.loc[specific_datetime]['[ID21.SG2] L_Low'])
                                case 'M':
                                    variables['NQ_M_HIGH'] = float(data.loc[specific_datetime]['[ID22.SG1] M_High'])
                                    variables['NQ_M_LOW'] = float(data.loc[specific_datetime]['[ID22.SG2] M_Low'])
                        else:
                            pass
                case "NQ_2":
                    variables['NQ_ETH_VWAP'] = float(data.iloc[0]['[ID7.SG1] ETH_VWAP'])
                    variables['NQ_ETH_VWAP_P2'] = float(data.iloc[2]['[ID7.SG1] ETH_VWAP'])
                    variables['NQ_ETH_TOP_1'] = float(data.iloc[0]['[ID7.SG2] Top_1'])
                    variables['NQ_ETH_BOTTOM_1'] = float(data.iloc[0]['[ID7.SG3] Bottom_1'])
                    variables['NQ_ETH_TOP_2'] = float(data.iloc[0]['[ID7.SG4] Top_2'])
                    variables['NQ_ETH_BOTTOM_2'] = float(data.iloc[0]['[ID7.SG5] Bottom_2'])
                    variables['NQ_CPL'] = float(data.iloc[0]['[ID2.SG1] CPL'])
                    variables['NQ_5D_VPOC'] = float(data.iloc[0]['[ID4.SG2] 5DVPOC'])
                    variables['NQ_20D_VPOC'] = float(data.iloc[0]['[ID3.SG2] 20DVPOC'])
                    variables['NQ_P_WOPEN'] = float(data.iloc[0]['[ID5.SG1] P_WOPEN'])
                    variables['NQ_P_WHIGH'] = float(data.iloc[0]['[ID5.SG2] P_WHIGH'])
                    variables['NQ_P_WLO'] = float(data.iloc[0]['[ID5.SG3] P_WLO'])
                    variables['NQ_P_WCLOSE'] = float(data.iloc[0]['[ID5.SG4] P_WCLOSE'])
                    variables['NQ_P_WVPOC'] = float(data.iloc[0]['[ID11.SG1] P_WVPOC'])
                    variables['NQ_WVWAP'] = float(data.iloc[0]['[ID10.SG1] WVWAP'])
                    variables['NQ_P_MOPEN'] = float(data.iloc[0]['[ID8.SG1] P_MOPEN'])
                    variables['NQ_P_MHIGH'] = float(data.iloc[0]['[ID8.SG2] P_MHIGH'])
                    variables['NQ_P_MLO'] = float(data.iloc[0]['[ID8.SG3] P_MLO'])
                    variables['NQ_P_MCLOSE'] = float(data.iloc[0]['[ID8.SG4] P_MCLOSE'])
                    variables['NQ_P_MVPOC'] = float(data.iloc[0]['[ID12.SG1] P_MVPOC'])
                    variables['NQ_MVWAP'] = float(data.iloc[0]['[ID1.SG1] MVWAP'])
               
                case "NQ_3":
                    variables['NQ_IB_ATR'] = float(data.iloc[1]['[ID2.SG1] IB ATR'])
                    variables['NQ_IB_HIGH'] = float(data.iloc[0]['[ID1.SG2] IBH'])
                    variables['NQ_IB_LOW'] = float(data.iloc[0]['[ID1.SG3] IBL'])
                    variables['NQ_PRIOR_IB_HIGH'] = float(data.iloc[1]['[ID1.SG2] IBH'])
                    variables['NQ_PRIOR_IB_LOW'] = float(data.iloc[1]['[ID1.SG3] IBL'])
                case "NQ_4":
                    variables['NQ_OVNH'] = float(data.iloc[0]['[ID1.SG2] OVN H'])
                    variables['NQ_OVNL'] = float(data.iloc[0]['[ID1.SG3] OVN L'])
                    variables['NQ_TOTAL_OVN_DELTA'] = float(data.iloc[0]['[ID3.SG4] OVN Total'])
                
                case "NQ_5":
                    variables['NQ_OVNTOIB_HI'] = float(data.iloc[0]['[ID1.SG2] OVNTOIB_HI'])
                    variables['NQ_OVNTOIB_LO'] = float(data.iloc[0]['[ID1.SG3] OVNTOIB_LO'])
                
                case "NQ_6":
                    variables['NQ_EURO_IBH'] = float(data.iloc[0]['[ID1.SG2] EURO IBH'])
                    variables['NQ_EURO_IBL'] = float(data.iloc[0]['[ID1.SG3] EURO IBL'])
                
                case "NQ_7":
                    variables['NQ_ORH'] = float(data.iloc[0]['[ID1.SG2] ORH'])
                    variables['NQ_ORL'] = float(data.iloc[0]['[ID1.SG3] ORL'])
                
                case "RTY_1":
                    # ------------------- Use Integer Based iLoc ----------------------- #
                    variables['RTY_DAY_OPEN'] = float(data.iloc[0]['[ID2.SG1] Day_Open'])
                    variables['RTY_DAY_HIGH'] = float(data.iloc[0]['[ID2.SG2] Day_High'])
                    variables['RTY_DAY_LOW'] = float(data.iloc[0]['[ID2.SG3] Day_Low'])
                    variables['RTY_DAY_CLOSE'] = float(data.iloc[0]['[ID2.SG4] Day_Close'])
                    variables['RTY_DAY_VPOC'] = float(data.iloc[0]['[ID1.SG1] Day_Vpoc'])
                    variables['RTY_PRIOR_VPOC'] = float(data.iloc[0]['[ID9.SG1] Prior_Vpoc'])
                    variables['RTY_PRIOR_HIGH'] = float(data.iloc[0]['[ID8.SG2] Prior_High'])
                    variables['RTY_PRIOR_LOW'] = float(data.iloc[0]['[ID8.SG3] Prior_Low'])
                    variables['RTY_PRIOR_CLOSE'] = float(data.iloc[0]['[ID8.SG4] Prior_Close'])
                    variables['RTY_RVOL'] = float(data.iloc[0]['[ID6.SG1] R_Vol'])
                    variables['RTY_CUMULATIVE_RVOL'] = float(data.iloc[0]['[ID6.SG2] R_Vol_Cumulative'])
                    variables['RTY_TOTAL_RTH_DELTA'] = float(data.iloc[0]['[ID4.SG4] Total_Delta'])
                    # ---------------------- Use Date Date Time Loc ------------------------- #
                    latest_date = data.index.max().date()
                    period_datetimes = {}

                    for period_label, period_time in period_equity.items():
                        specific_datetime = datetime.combine(latest_date, period_time)
                        period_datetimes[period_label] = specific_datetime

                    for period_label, specific_datetime in period_datetimes.items():    
                        if specific_datetime in data.index:
                            match period_label:
                                case 'A':
                                    variables['RTY_A_HIGH'] = float(data.loc[specific_datetime]['[ID10.SG1] A_High'])
                                    variables['RTY_A_LOW'] = float(data.loc[specific_datetime]['[ID10.SG2] A_Low'])    
                                case 'B':
                                    variables['RTY_B_HIGH'] = float(data.loc[specific_datetime]['[ID11.SG1] B_High'])
                                    variables['RTY_B_LOW'] = float(data.loc[specific_datetime]['[ID11.SG2] B_Low'])
                                case 'C':
                                    variables['RTY_C_HIGH'] = float(data.loc[specific_datetime]['[ID12.SG1] C_High'])
                                    variables['RTY_C_LOW'] = float(data.loc[specific_datetime]['[ID12.SG2] C_Low'])
                                case 'D':
                                    variables['RTY_D_HIGH'] = float(data.loc[specific_datetime]['[ID13.SG1] D_High'])
                                    variables['RTY_D_LOW'] = float(data.loc[specific_datetime]['[ID13.SG2] D_Low'])
                                case 'E':
                                    variables['RTY_E_HIGH'] = float(data.loc[specific_datetime]['[ID14.SG1] E_High'])
                                    variables['RTY_E_LOW'] = float(data.loc[specific_datetime]['[ID14.SG2] E_Low'])
                                case 'F':
                                    variables['RTY_F_HIGH'] = float(data.loc[specific_datetime]['[ID15.SG1] F_High'])
                                    variables['RTY_F_LOW'] = float(data.loc[specific_datetime]['[ID15.SG2] F_Low'])
                                case 'G':
                                    variables['RTY_G_HIGH'] = float(data.loc[specific_datetime]['[ID16.SG1] G_High'])
                                    variables['RTY_G_LOW'] = float(data.loc[specific_datetime]['[ID16.SG2] G_Low'])
                                case 'H':
                                    variables['RTY_H_HIGH'] = float(data.loc[specific_datetime]['[ID17.SG1] H_High'])
                                    variables['RTY_H_LOW'] = float(data.loc[specific_datetime]['[ID17.SG2] H_Low'])
                                case 'I':
                                    variables['RTY_I_HIGH'] = float(data.loc[specific_datetime]['[ID18.SG1] I_High'])
                                    variables['RTY_I_LOW'] = float(data.loc[specific_datetime]['[ID18.SG2] I_Low'])
                                case 'J':
                                    variables['RTY_J_HIGH'] = float(data.loc[specific_datetime]['[ID19.SG1] J_High'])
                                    variables['RTY_J_LOW'] = float(data.loc[specific_datetime]['[ID19.SG2] J_Low'])
                                case 'K':
                                    variables['RTY_K_HIGH'] = float(data.loc[specific_datetime]['[ID20.SG1] K_High'])
                                    variables['RTY_K_LOW'] = float(data.loc[specific_datetime]['[ID20.SG2] K_Low'])
                                case 'L':
                                    variables['RTY_L_HIGH'] = float(data.loc[specific_datetime]['[ID21.SG1] L_High'])
                                    variables['RTY_L_LOW'] = float(data.loc[specific_datetime]['[ID21.SG2] L_Low'])
                                case 'M':
                                    variables['RTY_M_HIGH'] = float(data.loc[specific_datetime]['[ID22.SG1] M_High'])
                                    variables['RTY_M_LOW'] = float(data.loc[specific_datetime]['[ID22.SG2] M_Low'])
                        else:
                            pass
                case "RTY_2":
                    variables['RTY_ETH_VWAP'] = float(data.iloc[0]['[ID7.SG1] ETH_VWAP'])
                    variables['RTY_ETH_VWAP_P2'] = float(data.iloc[2]['[ID7.SG1] ETH_VWAP'])
                    variables['RTY_ETH_TOP_1'] = float(data.iloc[0]['[ID7.SG2] Top_1'])
                    variables['RTY_ETH_BOTTOM_1'] = float(data.iloc[0]['[ID7.SG3] Bottom_1'])
                    variables['RTY_ETH_TOP_2'] = float(data.iloc[0]['[ID7.SG4] Top_2'])
                    variables['RTY_ETH_BOTTOM_2'] = float(data.iloc[0]['[ID7.SG5] Bottom_2'])
                    variables['RTY_CPL'] = float(data.iloc[0]['[ID2.SG1] CPL'])
                    variables['RTY_5D_VPOC'] = float(data.iloc[0]['[ID4.SG2] 5DVPOC'])
                    variables['RTY_20D_VPOC'] = float(data.iloc[0]['[ID3.SG2] 20DVPOC'])
                    variables['RTY_P_WOPEN'] = float(data.iloc[0]['[ID5.SG1] P_WOPEN'])
                    variables['RTY_P_WHIGH'] = float(data.iloc[0]['[ID5.SG2] P_WHIGH'])
                    variables['RTY_P_WLO'] = float(data.iloc[0]['[ID5.SG3] P_WLO'])
                    variables['RTY_P_WCLOSE'] = float(data.iloc[0]['[ID5.SG4] P_WCLOSE'])
                    variables['RTY_P_WVPOC'] = float(data.iloc[0]['[ID11.SG1] P_WVPOC'])
                    variables['RTY_WVWAP'] = float(data.iloc[0]['[ID10.SG1] WVWAP'])
                    variables['RTY_P_MOPEN'] = float(data.iloc[0]['[ID8.SG1] P_MOPEN'])
                    variables['RTY_P_MHIGH'] = float(data.iloc[0]['[ID8.SG2] P_MHIGH'])
                    variables['RTY_P_MLO'] = float(data.iloc[0]['[ID8.SG3] P_MLO'])
                    variables['RTY_P_MCLOSE'] = float(data.iloc[0]['[ID8.SG4] P_MCLOSE'])
                    variables['RTY_P_MVPOC'] = float(data.iloc[0]['[ID12.SG1] P_MVPOC'])
                    variables['RTY_MVWAP'] = float(data.iloc[0]['[ID1.SG1] MVWAP'])
               
                case "RTY_3":
                    variables['RTY_IB_ATR'] = float(data.iloc[1]['[ID2.SG1] IB ATR'])
                    variables['RTY_IB_HIGH'] = float(data.iloc[0]['[ID1.SG2] IBH'])
                    variables['RTY_IB_LOW'] = float(data.iloc[0]['[ID1.SG3] IBL'])
                    variables['RTY_PRIOR_IB_HIGH'] = float(data.iloc[1]['[ID1.SG2] IBH'])
                    variables['RTY_PRIOR_IB_LOW'] = float(data.iloc[1]['[ID1.SG3] IBL'])
                case "RTY_4":
                    variables['RTY_OVNH'] = float(data.iloc[0]['[ID1.SG2] OVN H'])
                    variables['RTY_OVNL'] = float(data.iloc[0]['[ID1.SG3] OVN L'])
                    variables['RTY_TOTAL_OVN_DELTA'] = float(data.iloc[0]['[ID3.SG4] OVN Total'])
                
                case "RTY_5":
                    variables['RTY_OVNTOIB_HI'] = float(data.iloc[0]['[ID1.SG2] OVNTOIB_HI'])
                    variables['RTY_OVNTOIB_LO'] = float(data.iloc[0]['[ID1.SG3] OVNTOIB_LO'])
                
                case "RTY_6":
                    variables['RTY_EURO_IBH'] = float(data.iloc[0]['[ID1.SG2] EURO IBH'])
                    variables['RTY_EURO_IBL'] = float(data.iloc[0]['[ID1.SG3] EURO IBL'])
                
                case "RTY_7":
                    variables['RTY_ORH'] = float(data.iloc[0]['[ID1.SG2] ORH'])
                    variables['RTY_ORL'] = float(data.iloc[0]['[ID1.SG3] ORL'])
                
                case "CL_1":
                    # ------------------- Use Integer Based iLoc ----------------------- #
                    variables['CL_DAY_OPEN'] = float(data.iloc[0]['[ID2.SG1] Day_Open'])
                    variables['CL_DAY_HIGH'] = float(data.iloc[0]['[ID2.SG2] Day_High'])
                    variables['CL_DAY_LOW'] = float(data.iloc[0]['[ID2.SG3] Day_Low'])
                    variables['CL_DAY_CLOSE'] = float(data.iloc[0]['[ID2.SG4] Day_Close'])
                    variables['CL_DAY_VPOC'] = float(data.iloc[0]['[ID1.SG1] Day_Vpoc'])
                    variables['CL_PRIOR_VPOC'] = float(data.iloc[0]['[ID9.SG1] Prior_Vpoc'])
                    variables['CL_PRIOR_HIGH'] = float(data.iloc[0]['[ID8.SG2] Prior_High'])
                    variables['CL_PRIOR_LOW'] = float(data.iloc[0]['[ID8.SG3] Prior_Low'])
                    variables['CL_PRIOR_CLOSE'] = float(data.iloc[0]['[ID8.SG4] Prior_Close'])
                    variables['CL_RVOL'] = float(data.iloc[0]['[ID6.SG1] R_Vol'])
                    variables['CL_CUMULATIVE_RVOL'] = float(data.iloc[0]['[ID6.SG2] R_Vol_Cumulative'])
                    variables['CL_TOTAL_RTH_DELTA'] = float(data.iloc[0]['[ID4.SG4] Total_Delta'])
                    # ---------------------- Use Date Date Time Loc ------------------------- #
                    latest_date = data.index.max().date()
                    period_datetimes = {}

                    for period_label, period_time in period_crude.items():
                        specific_datetime = datetime.combine(latest_date, period_time)
                        period_datetimes[period_label] = specific_datetime

                    for period_label, specific_datetime in period_datetimes.items():    
                        if specific_datetime in data.index:
                            match period_label:
                                case 'A':
                                    variables['CL_A_HIGH'] = float(data.loc[specific_datetime]['[ID10.SG1] A_High'])
                                    variables['CL_A_LOW'] = float(data.loc[specific_datetime]['[ID10.SG2] A_Low'])    
                                case 'B':
                                    variables['CL_B_HIGH'] = float(data.loc[specific_datetime]['[ID11.SG1] B_High'])
                                    variables['CL_B_LOW'] = float(data.loc[specific_datetime]['[ID11.SG2] B_Low'])
                                case 'C':
                                    variables['CL_C_HIGH'] = float(data.loc[specific_datetime]['[ID12.SG1] C_High'])
                                    variables['CL_C_LOW'] = float(data.loc[specific_datetime]['[ID12.SG2] C_Low'])
                                case 'D':
                                    variables['CL_D_HIGH'] = float(data.loc[specific_datetime]['[ID13.SG1] D_High'])
                                    variables['CL_D_LOW'] = float(data.loc[specific_datetime]['[ID13.SG2] D_Low'])
                                case 'E':
                                    variables['CL_E_HIGH'] = float(data.loc[specific_datetime]['[ID14.SG1] E_High'])
                                    variables['CL_E_LOW'] = float(data.loc[specific_datetime]['[ID14.SG2] E_Low'])
                                case 'F':
                                    variables['CL_F_HIGH'] = float(data.loc[specific_datetime]['[ID15.SG1] F_High'])
                                    variables['CL_F_LOW'] = float(data.loc[specific_datetime]['[ID15.SG2] F_Low'])
                                case 'G':
                                    variables['CL_G_HIGH'] = float(data.loc[specific_datetime]['[ID16.SG1] G_High'])
                                    variables['CL_G_LOW'] = float(data.loc[specific_datetime]['[ID16.SG2] G_Low'])
                                case 'H':
                                    variables['CL_H_HIGH'] = float(data.loc[specific_datetime]['[ID17.SG1] H_High'])
                                    variables['CL_H_LOW'] = float(data.loc[specific_datetime]['[ID17.SG2] H_Low'])
                                case 'I':
                                    variables['CL_I_HIGH'] = float(data.loc[specific_datetime]['[ID18.SG1] I_High'])
                                    variables['CL_I_LOW'] = float(data.loc[specific_datetime]['[ID18.SG2] I_Low'])
                                case 'J':
                                    variables['CL_J_HIGH'] = float(data.loc[specific_datetime]['[ID19.SG1] J_High'])
                                    variables['CL_J_LOW'] = float(data.loc[specific_datetime]['[ID19.SG2] J_Low'])
                                case 'K':
                                    variables['CL_K_HIGH'] = float(data.loc[specific_datetime]['[ID20.SG1] K_High'])
                                    variables['CL_K_LOW'] = float(data.loc[specific_datetime]['[ID20.SG2] K_Low'])
                        else:
                            pass
                case "CL_2":
                    variables['CL_ETH_VWAP'] = float(data.iloc[0]['[ID7.SG1] ETH_VWAP'])
                    variables['CL_ETH_VWAP_P2'] = float(data.iloc[2]['[ID7.SG1] ETH_VWAP'])
                    variables['CL_ETH_TOP_1'] = float(data.iloc[0]['[ID7.SG2] Top_1'])
                    variables['CL_ETH_BOTTOM_1'] = float(data.iloc[0]['[ID7.SG3] Bottom_1'])
                    variables['CL_ETH_TOP_2'] = float(data.iloc[0]['[ID7.SG4] Top_2'])
                    variables['CL_ETH_BOTTOM_2'] = float(data.iloc[0]['[ID7.SG5] Bottom_2'])
                    variables['CL_CPL'] = float(data.iloc[0]['[ID2.SG1] CPL'])
                    variables['CL_5D_VPOC'] = float(data.iloc[0]['[ID4.SG2] 5DVPOC'])
                    variables['CL_20D_VPOC'] = float(data.iloc[0]['[ID3.SG2] 20DVPOC'])
                    variables['CL_P_WOPEN'] = float(data.iloc[0]['[ID5.SG1] P_WOPEN'])
                    variables['CL_P_WHIGH'] = float(data.iloc[0]['[ID5.SG2] P_WHIGH'])
                    variables['CL_P_WLO'] = float(data.iloc[0]['[ID5.SG3] P_WLO'])
                    variables['CL_P_WCLOSE'] = float(data.iloc[0]['[ID5.SG4] P_WCLOSE'])
                    variables['CL_P_WVPOC'] = float(data.iloc[0]['[ID11.SG1] P_WVPOC'])
                    variables['CL_WVWAP'] = float(data.iloc[0]['[ID10.SG1] WVWAP'])
                    variables['CL_P_MOPEN'] = float(data.iloc[0]['[ID8.SG1] P_MOPEN'])
                    variables['CL_P_MHIGH'] = float(data.iloc[0]['[ID8.SG2] P_MHIGH'])
                    variables['CL_P_MLO'] = float(data.iloc[0]['[ID8.SG3] P_MLO'])
                    variables['CL_P_MCLOSE'] = float(data.iloc[0]['[ID8.SG4] P_MCLOSE'])
                    variables['CL_P_MVPOC'] = float(data.iloc[0]['[ID12.SG1] P_MVPOC'])
                    variables['CL_MVWAP'] = float(data.iloc[0]['[ID1.SG1] MVWAP'])
                
                case "CL_3":
                    variables['CL_IB_ATR'] = float(data.iloc[1]['[ID2.SG1] IB ATR'])
                    variables['CL_IB_HIGH'] = float(data.iloc[0]['[ID1.SG2] IBH'])
                    variables['CL_IB_LOW'] = float(data.iloc[0]['[ID1.SG3] IBL'])
                    variables['CL_PRIOR_IB_HIGH'] = float(data.iloc[1]['[ID1.SG2] IBH'])
                    variables['CL_PRIOR_IB_LOW'] = float(data.iloc[1]['[ID1.SG3] IBL'])                
                case "CL_4":
                    variables['CL_OVNH'] = float(data.iloc[0]['[ID1.SG2] OVN H'])
                    variables['CL_OVNL'] = float(data.iloc[0]['[ID1.SG3] OVN L'])
                    variables['CL_TOTAL_OVN_DELTA'] = float(data.iloc[0]['[ID3.SG4] OVN Total'])
                
                case "CL_5":
                    variables['CL_OVNTOIB_HI'] = float(data.iloc[0]['[ID1.SG2] OVNTOIB_HI'])
                    variables['CL_OVNTOIB_LO'] = float(data.iloc[0]['[ID1.SG3] OVNTOIB_LO'])
                
                case "CL_6":
                    variables['CL_EURO_IBH'] = float(data.iloc[0]['[ID1.SG2] EURO IBH'])
                    variables['CL_EURO_IBL'] = float(data.iloc[0]['[ID1.SG3] EURO IBL'])

                case "CL_7":
                    variables['CL_ORH'] = float(data.iloc[0]['[ID1.SG2] ORH'])
                    variables['CL_ORL'] = float(data.iloc[0]['[ID1.SG3] ORL'])
                
                case other:
                    logger.error(" Startup | prep_data | Note: No Such Switch Location")
                   
            product_name = task["name"].split('_')[0]
            if product_name not in all_variables:
                all_variables[product_name] = {}

            all_variables[product_name].update(variables)  
        logger.debug(f" Startup | prep_data | Product: {product_name} | Variables: {all_variables}")
        return all_variables 
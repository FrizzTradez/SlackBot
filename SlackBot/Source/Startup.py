import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import logging
from SlackBot.Static.Lists import *

logger = logging.getLogger(__name__)

scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file(r"SlackBot\External\Credentials.json", scopes=scopes)
client = gspread.authorize(creds)

class Initialization:
    def grab_impvol(external_params):
        
        output_dict = {}

        for task in external_params:
            workbook = client.open_by_key(task["sheet_id"])
            sheet = workbook.worksheet(task["sheet_name"])
            cell_value = sheet.cell(task["row_number"], task["col_number"]).value
            logger.debug(f"Fetching ImpVol | Sheet Name : {task['sheet_name']} | Row Number : {task['row_number']}  | Col Number : {task['col_number']}")
            
            if "ES" in task["sheet_name"]:
                output_dict['es_impvol'] = cell_value
            elif "NQ" in task["sheet_name"]:
                output_dict['nq_impvol'] = cell_value
            elif "RTY" in task["sheet_name"]:
                output_dict['rty_impvol'] = cell_value
            elif "CL" in task["sheet_name"]:
                output_dict['cl_impvol'] = cell_value
                
        es_impvol = float(output_dict['es_impvol'].strip('%'))
        nq_impvol = float(output_dict['nq_impvol'].strip('%'))
        rty_impvol = float(output_dict['rty_impvol'].strip('%'))
        cl_impvol = float(output_dict['cl_impvol'].strip('%'))
        logger.debug(f"ES_ImpVol : {es_impvol} | NQ_ImpVol : {nq_impvol}| RTY_ImpVol : {rty_impvol}| CL_ImpVol : {cl_impvol}")
        
        return es_impvol, nq_impvol, rty_impvol, cl_impvol

    def prep_data(files):
        all_variables = {}

        for task in files:  
            if task["header_row"] == 1:
                data = pd.read_csv(task["filepath"], delimiter='\t', header=None)
            elif task["header_row"] == 0:
                data = pd.read_csv(task["filepath"], delimiter='\t')
                data = data.reset_index()
            else:
                raise ValueError("header_row should be either 0 or 1")
            
            logger.debug(f"Data after reading CSV:\n{data.head()}")
            
            # Set Header and Configure DF
            pd.options.mode.copy_on_write = True
            data.to_string()
            data.columns = data.iloc[task["iloc1"]]
            data = data[task["iloc2"]:]
            
            logger.debug(f"Data after setting headers and slicing:\n{data.head()}")
            
            # Drop Unwanted Columns and N/A
            existing_columns_to_drop = [col for col in columns_to_drop if col in data.columns]
            data = data.drop(columns=existing_columns_to_drop)
            df_cleaned = data.loc[:, data.columns.notna()]
            data = df_cleaned
            data = data.dropna()
            
            logger.debug(f"Data after dropping columns and NA:\n{data.head()}")
            
            # Set Date-Time As Index
            if 'Date Time' in data.columns:
                data['Date Time'] = pd.to_datetime(data['Date Time'], errors='coerce')
                data.set_index('Date Time', inplace=True)
                
                logger.debug(f"Data after setting 'Date Time' as index:\n{data.head()}")  
              
            # Converting Columns to float
            for columns in task["columns"]:
                data[columns] = data[columns].str.replace(',', '.').astype(float)
            
            # Get Necessary Variables  
            variables = {}

            match task["name"]:
                case "ES_1":
                    variables['ES_DAY_OPEN'] = float(data.loc[2, '[ID2.SG1] Day_Open'])
                    variables['ES_DAY_HIGH'] = float(data.loc[2, '[ID2.SG2] Day_High'])
                    variables['ES_DAY_LOW'] = float(data.loc[2, '[ID2.SG3] Day_Low'])
                    variables['ES_DAY_CLOSE'] = float(data.loc[2, '[ID2.SG4] Day_Close'])
                    variables['ES_DAY_VPOC'] = float(data.loc[2, '[ID1.SG1] Day_Vpoc'])
                    variables['ES_PRIOR_VPOC'] = float(data.loc[2, '[ID9.SG1] Prior_Vpoc'])
                    variables['ES_PRIOR_HIGH'] = float(data.loc[2, '[ID8.SG2] Prior_High'])
                    variables['ES_PRIOR_LOW'] = float(data.loc[2, '[ID8.SG3] Prior_Low'])
                    variables['ES_PRIOR_CLOSE'] = float(data.loc[2, '[ID8.SG4] Prior_Close'])
                    variables['ES_RVOL'] = float(data.loc[2, '[ID6.SG1] R_Vol'])
                    variables['ES_CUMULATIVE_RVOL'] = float(data.loc[2, '[ID6.SG2] R_Vol_Cumulative'])
                    variables['ES_TOTAL_RTH_DELTA'] = float(data.loc[2, '[ID4.SG4] Total_Delta'])
                    '''
                    variables['ES_A_HIGH'] = float(data.loc[2, '[ID10.SG1] A_High'])
                    variables['ES_A_LOW'] = float(data.loc[2, '[ID10.SG2] A_Low'])
                    variables['ES_B_HIGH'] = float(data.loc[2, '[ID11.SG1] B_High'])
                    variables['ES_B_LOW'] = float(data.loc[2, '[ID11.SG2] B_Low'])
                    variables['ES_C_HIGH'] = float(data.loc[2, '[ID12.SG1] C_High'])
                    variables['ES_C_LOW'] = float(data.loc[2, '[ID12.SG2] C_Low'])
                    variables['ES_D_HIGH'] = float(data.loc[2, '[ID13.SG1] D_High'])
                    variables['ES_D_LOW'] = float(data.loc[2, '[ID13.SG2] D_Low'])
                    variables['ES_E_HIGH'] = float(data.loc[2, '[ID14.SG1] E_High'])
                    variables['ES_E_LOW'] = float(data.loc[2, '[ID14.SG2] E_Low'])
                    variables['ES_F_HIGH'] = float(data.loc[2, '[ID15.SG1] F_High'])
                    variables['ES_F_LOW'] = float(data.loc[2, '[ID15.SG2] F_Low'])
                    variables['ES_G_HIGH'] = float(data.loc[2, '[ID16.SG1] G_High'])
                    variables['ES_G_LOW'] = float(data.loc[2, '[ID16.SG2] G_Low'])
                    variables['ES_H_HIGH'] = float(data.loc[2, '[ID17.SG1] H_High'])
                    variables['ES_H_LOW'] = float(data.loc[2, '[ID17.SG2] H_Low'])
                    variables['ES_I_HIGH'] = float(data.loc[2, '[ID18.SG1] I_High'])
                    variables['ES_I_LOW'] = float(data.loc[2, '[ID18.SG2] I_Low'])
                    variables['ES_J_HIGH'] = float(data.loc[2, '[ID19.SG1] J_High'])
                    variables['ES_J_LOW'] = float(data.loc[2, '[ID19.SG2] J_Low'])                
                    variables['ES_K_HIGH'] = float(data.loc[2, '[ID20.SG1] K_High'])
                    variables['ES_K_LOW'] = float(data.loc[2, '[ID20.SG2] K_Low'])                
                    variables['ES_L_HIGH'] = float(data.loc[2, '[ID21.SG1] L_High'])
                    variables['ES_L_LOW'] = float(data.loc[2, '[ID21.SG2] L_Low'])                
                    variables['ES_M_HIGH'] = float(data.loc[2, '[ID22.SG1] M_High'])
                    variables['ES_M_LOW'] = float(data.loc[2, '[ID22.SG2] M_Low'])                
                    '''
                case "ES_2":
                    variables['ES_ETH_VWAP'] = float(data.loc[2, '[ID7.SG1] ETH_VWAP'])
                    variables['ES_ETH_TOP_1'] = float(data.loc[2, '[ID7.SG2] Top_1'])
                    variables['ES_ETH_BOTTOM_1'] = float(data.loc[2, '[ID7.SG3] Bottom_1'])
                    variables['ES_ETH_TOP_2'] = float(data.loc[2, '[ID7.SG4] Top_2'])
                    variables['ES_ETH_BOTTOM_2'] = float(data.loc[2, '[ID7.SG5] Bottom_2'])
                    variables['ES_CPL'] = float(data.loc[2, '[ID2.SG1] CPL'])
                    variables['ES_5D_VPOC'] = float(data.loc[2, '[ID4.SG2] 5DVPOC'])
                    variables['ES_20D_VPOC'] = float(data.loc[2, '[ID3.SG2] 20DVPOC'])
                    variables['ES_P_WOPEN'] = float(data.loc[2, '[ID5.SG1] P_WOPEN'])
                    variables['ES_P_WHIGH'] = float(data.loc[2, '[ID5.SG2] P_WHIGH'])
                    variables['ES_P_WLO'] = float(data.loc[2, '[ID5.SG3] P_WLO'])
                    variables['ES_P_WCLOSE'] = float(data.loc[2, '[ID5.SG4] P_WCLOSE'])
                    variables['ES_P_WVPOC'] = float(data.loc[2, '[ID11.SG1] P_WVPOC'])
                    variables['ES_WVWAP'] = float(data.loc[2, '[ID10.SG1] WVWAP'])
                    variables['ES_P_MOPEN'] = float(data.loc[2, '[ID8.SG1] P_MOPEN'])
                    variables['ES_P_MHIGH'] = float(data.loc[2, '[ID8.SG2] P_MHIGH'])
                    variables['ES_P_MLO'] = float(data.loc[2, '[ID8.SG3] P_MLO'])
                    variables['ES_P_MCLOSE'] = float(data.loc[2, '[ID8.SG4] P_MCLOSE'])
                    variables['ES_P_MVPOC'] = float(data.loc[2, '[ID12.SG1] P_MVPOC'])
                    variables['ES_MVWAP'] = float(data.loc[2, '[ID1.SG1] MVWAP'])
                case "ES_3":
                    variables['ES_IB_ATR'] = float(data.loc[2, '[ID2.SG1] IB ATR'])
                    variables['ES_IB_HIGH'] = float(data.loc[1, '[ID1.SG2] IBH'])
                    variables['ES_IB_LOW'] = float(data.loc[1, '[ID1.SG3] IBL'])
                case "ES_4":
                    variables['ES_OVNH'] = float(data.loc[2, '[ID1.SG2] OVN H'])
                    variables['ES_OVNL'] = float(data.loc[2, '[ID1.SG3] OVN L'])
                    variables['ES_TOTAL_OVN_DELTA'] = float(data.loc[2, '[ID3.SG4] OVN Total'])
                case "ES_5":
                    variables['ES_OVNTOIB_HI'] = float(data.loc[1, '[ID1.SG2] OVNTOIB_HI'])
                    variables['ES_OVNTOIB_LO'] = float(data.loc[1, '[ID1.SG3] OVNTOIB_LO'])
                case "ES_6":
                    variables['ES_EURO_IBH'] = float(data.loc[1, '[ID1.SG2] EURO IBH'])
                    variables['ES_EURO_IBL'] = float(data.loc[1, '[ID1.SG3] EURO IBL'])
                case "ES_7":
                    variables['ES_ORH'] = float(data.loc[1, '[ID1.SG2] ORH'])
                    variables['ES_ORL'] = float(data.loc[1, '[ID1.SG3] ORL'])
                case "NQ_1":
                    variables['NQ_DAY_OPEN'] = float(data.loc[2, '[ID2.SG1] Day_Open'])
                    variables['NQ_DAY_HIGH'] = float(data.loc[2, '[ID2.SG2] Day_High'])
                    variables['NQ_DAY_LOW'] = float(data.loc[2, '[ID2.SG3] Day_Low'])
                    variables['NQ_DAY_CLOSE'] = float(data.loc[2, '[ID2.SG4] Day_Close'])
                    variables['NQ_DAY_VPOC'] = float(data.loc[2, '[ID1.SG1] Day_Vpoc'])
                    variables['NQ_PRIOR_VPOC'] = float(data.loc[2, '[ID9.SG1] Prior_Vpoc'])
                    variables['NQ_PRIOR_HIGH'] = float(data.loc[2, '[ID8.SG2] Prior_High'])
                    variables['NQ_PRIOR_LOW'] = float(data.loc[2, '[ID8.SG3] Prior_Low'])
                    variables['NQ_PRIOR_CLOSE'] = float(data.loc[2, '[ID8.SG4] Prior_Close'])
                    variables['NQ_RVOL'] = float(data.loc[2, '[ID6.SG1] R_Vol'])
                    variables['NQ_CUMULATIVE_RVOL'] = float(data.loc[2, '[ID6.SG2] R_Vol_Cumulative'])
                    variables['NQ_TOTAL_RTH_DELTA'] = float(data.loc[2, '[ID4.SG4] Total_Delta'])

                    # FIGURE OUT A WAY TO MAKE THESE COSISTENT AND LOCATABLE
                    variables['NQ_A_HIGH'] = float(data.loc[2, '[ID10.SG1] A_High'])
                    variables['NQ_A_LOW'] = float(data.loc[2, '[ID10.SG2] A_Low'])
                    variables['NQ_B_HIGH'] = float(data.loc[2, '[ID11.SG1] B_High'])
                    variables['NQ_B_LOW'] = float(data.loc[2, '[ID11.SG2] B_Low'])
                    variables['NQ_C_HIGH'] = float(data.loc[2, '[ID12.SG1] C_High'])
                    variables['NQ_C_LOW'] = float(data.loc[2, '[ID12.SG2] C_Low'])
                    variables['NQ_D_HIGH'] = float(data.loc[2, '[ID13.SG1] D_High'])
                    variables['NQ_D_LOW'] = float(data.loc[2, '[ID13.SG2] D_Low'])
                    variables['NQ_E_HIGH'] = float(data.loc[2, '[ID14.SG1] E_High'])
                    variables['NQ_E_LOW'] = float(data.loc[2, '[ID14.SG2] E_Low'])
                    variables['NQ_F_HIGH'] = float(data.loc[2, '[ID15.SG1] F_High'])
                    variables['NQ_F_LOW'] = float(data.loc[2, '[ID15.SG2] F_Low'])
                    variables['NQ_G_HIGH'] = float(data.loc[2, '[ID16.SG1] G_High'])
                    variables['NQ_G_LOW'] = float(data.loc[2, '[ID16.SG2] G_Low'])
                    variables['NQ_H_HIGH'] = float(data.loc[2, '[ID17.SG1] H_High'])
                    variables['NQ_H_LOW'] = float(data.loc[2, '[ID17.SG2] H_Low'])
                    variables['NQ_I_HIGH'] = float(data.loc[2, '[ID18.SG1] I_High'])
                    variables['NQ_I_LOW'] = float(data.loc[2, '[ID18.SG2] I_Low'])
                    variables['NQ_J_HIGH'] = float(data.loc[2, '[ID19.SG1] J_High'])
                    variables['NQ_J_LOW'] = float(data.loc[2, '[ID19.SG2] J_Low'])                
                    variables['NQ_K_HIGH'] = float(data.loc[2, '[ID20.SG1] K_High'])
                    variables['NQ_K_LOW'] = float(data.loc[2, '[ID20.SG2] K_Low'])                
                    variables['NQ_L_HIGH'] = float(data.loc[2, '[ID21.SG1] L_High'])
                    variables['NQ_L_LOW'] = float(data.loc[2, '[ID21.SG2] L_Low'])                
                    variables['NQ_M_HIGH'] = float(data.loc[2, '[ID22.SG1] M_High'])
                    variables['NQ_M_LOW'] = float(data.loc[2, '[ID22.SG2] M_Low'])                
                
                case "NQ_2":
                    variables['NQ_ETH_VWAP'] = float(data.loc[2, '[ID7.SG1] ETH_VWAP'])
                    variables['NQ_ETH_TOP_1'] = float(data.loc[2, '[ID7.SG2] Top_1'])
                    variables['NQ_ETH_BOTTOM_1'] = float(data.loc[2, '[ID7.SG3] Bottom_1'])
                    variables['NQ_ETH_TOP_2'] = float(data.loc[2, '[ID7.SG4] Top_2'])
                    variables['NQ_ETH_BOTTOM_2'] = float(data.loc[2, '[ID7.SG5] Bottom_2'])
                    variables['NQ_CPL'] = float(data.loc[2, '[ID2.SG1] CPL'])
                    variables['NQ_5D_VPOC'] = float(data.loc[2, '[ID4.SG2] 5DVPOC'])
                    variables['NQ_20D_VPOC'] = float(data.loc[2, '[ID3.SG2] 20DVPOC'])
                    variables['NQ_P_WOPEN'] = float(data.loc[2, '[ID5.SG1] P_WOPEN'])
                    variables['NQ_P_WHIGH'] = float(data.loc[2, '[ID5.SG2] P_WHIGH'])
                    variables['NQ_P_WLO'] = float(data.loc[2, '[ID5.SG3] P_WLO'])
                    variables['NQ_P_WCLOSE'] = float(data.loc[2, '[ID5.SG4] P_WCLOSE'])
                    variables['NQ_P_WVPOC'] = float(data.loc[2, '[ID11.SG1] P_WVPOC'])
                    variables['NQ_WVWAP'] = float(data.loc[2, '[ID10.SG1] WVWAP'])
                    variables['NQ_P_MOPEN'] = float(data.loc[2, '[ID8.SG1] P_MOPEN'])
                    variables['NQ_P_MHIGH'] = float(data.loc[2, '[ID8.SG2] P_MHIGH'])
                    variables['NQ_P_MLO'] = float(data.loc[2, '[ID8.SG3] P_MLO'])
                    variables['NQ_P_MCLOSE'] = float(data.loc[2, '[ID8.SG4] P_MCLOSE'])
                    variables['NQ_P_MVPOC'] = float(data.loc[2, '[ID12.SG1] P_MVPOC'])
                    variables['NQ_MVWAP'] = float(data.loc[2, '[ID1.SG1] MVWAP'])
               
                case "NQ_3":
                    variables['NQ_IB_ATR'] = float(data.loc[2, '[ID2.SG1] IB ATR'])
                    variables['NQ_IB_HIGH'] = float(data.loc[1, '[ID1.SG2] IBH'])
                    variables['NQ_IB_LOW'] = float(data.loc[1, '[ID1.SG3] IBL'])
                 
                case "NQ_4":
                    variables['NQ_OVNH'] = float(data.loc[2, '[ID1.SG2] OVN H'])
                    variables['NQ_OVNL'] = float(data.loc[2, '[ID1.SG3] OVN L'])
                    variables['NQ_TOTAL_OVN_DELTA'] = float(data.loc[2, '[ID3.SG4] OVN Total'])
                
                case "NQ_5":
                    variables['NQ_OVNTOIB_HI'] = float(data.loc[1, '[ID1.SG2] OVNTOIB_HI'])
                    variables['NQ_OVNTOIB_LO'] = float(data.loc[1, '[ID1.SG3] OVNTOIB_LO'])
                
                case "NQ_6":
                    variables['NQ_EURO_IBH'] = float(data.loc[1, '[ID1.SG2] EURO IBH'])
                    variables['NQ_EURO_IBL'] = float(data.loc[1, '[ID1.SG3] EURO IBL'])
                
                case "NQ_7":
                    variables['NQ_ORH'] = float(data.loc[1, '[ID1.SG2] ORH'])
                    variables['NQ_ORL'] = float(data.loc[1, '[ID1.SG3] ORL'])
                
                case "RTY_1":
                    variables['RTY_DAY_OPEN'] = float(data.loc[2, '[ID2.SG1] Day_Open'])
                    variables['RTY_DAY_HIGH'] = float(data.loc[2, '[ID2.SG2] Day_High'])
                    variables['RTY_DAY_LOW'] = float(data.loc[2, '[ID2.SG3] Day_Low'])
                    variables['RTY_DAY_CLOSE'] = float(data.loc[2, '[ID2.SG4] Day_Close'])
                    variables['RTY_DAY_VPOC'] = float(data.loc[2, '[ID1.SG1] Day_Vpoc'])
                    variables['RTY_PRIOR_VPOC'] = float(data.loc[2, '[ID9.SG1] Prior_Vpoc'])
                    variables['RTY_PRIOR_HIGH'] = float(data.loc[2, '[ID8.SG2] Prior_High'])
                    variables['RTY_PRIOR_LOW'] = float(data.loc[2, '[ID8.SG3] Prior_Low'])
                    variables['RTY_PRIOR_CLOSE'] = float(data.loc[2, '[ID8.SG4] Prior_Close'])
                    variables['RTY_RVOL'] = float(data.loc[2, '[ID6.SG1] R_Vol'])
                    variables['RTY_CUMULATIVE_RVOL'] = float(data.loc[2, '[ID6.SG2] R_Vol_Cumulative'])
                    variables['RTY_TOTAL_RTH_DELTA'] = float(data.loc[2, '[ID4.SG4] Total_Delta'])

                    # FIGURE OUT A WAY TO MAKE THESE COSISTENT AND LOCATABLE
                    variables['RTY_A_HIGH'] = float(data.loc[2, '[ID10.SG1] A_High'])
                    variables['RTY_A_LOW'] = float(data.loc[2, '[ID10.SG2] A_Low'])
                    variables['RTY_B_HIGH'] = float(data.loc[2, '[ID11.SG1] B_High'])
                    variables['RTY_B_LOW'] = float(data.loc[2, '[ID11.SG2] B_Low'])
                    variables['RTY_C_HIGH'] = float(data.loc[2, '[ID12.SG1] C_High'])
                    variables['RTY_C_LOW'] = float(data.loc[2, '[ID12.SG2] C_Low'])
                    variables['RTY_D_HIGH'] = float(data.loc[2, '[ID13.SG1] D_High'])
                    variables['RTY_D_LOW'] = float(data.loc[2, '[ID13.SG2] D_Low'])
                    variables['RTY_E_HIGH'] = float(data.loc[2, '[ID14.SG1] E_High'])
                    variables['RTY_E_LOW'] = float(data.loc[2, '[ID14.SG2] E_Low'])
                    variables['RTY_F_HIGH'] = float(data.loc[2, '[ID15.SG1] F_High'])
                    variables['RTY_F_LOW'] = float(data.loc[2, '[ID15.SG2] F_Low'])
                    variables['RTY_G_HIGH'] = float(data.loc[2, '[ID16.SG1] G_High'])
                    variables['RTY_G_LOW'] = float(data.loc[2, '[ID16.SG2] G_Low'])
                    variables['RTY_H_HIGH'] = float(data.loc[2, '[ID17.SG1] H_High'])
                    variables['RTY_H_LOW'] = float(data.loc[2, '[ID17.SG2] H_Low'])
                    variables['RTY_I_HIGH'] = float(data.loc[2, '[ID18.SG1] I_High'])
                    variables['RTY_I_LOW'] = float(data.loc[2, '[ID18.SG2] I_Low'])
                    variables['RTY_J_HIGH'] = float(data.loc[2, '[ID19.SG1] J_High'])
                    variables['RTY_J_LOW'] = float(data.loc[2, '[ID19.SG2] J_Low'])                
                    variables['RTY_K_HIGH'] = float(data.loc[2, '[ID20.SG1] K_High'])
                    variables['RTY_K_LOW'] = float(data.loc[2, '[ID20.SG2] K_Low'])                
                    variables['RTY_L_HIGH'] = float(data.loc[2, '[ID21.SG1] L_High'])
                    variables['RTY_L_LOW'] = float(data.loc[2, '[ID21.SG2] L_Low'])                
                    variables['RTY_M_HIGH'] = float(data.loc[2, '[ID22.SG1] M_High'])
                    variables['RTY_M_LOW'] = float(data.loc[2, '[ID22.SG2] M_Low'])                
                
                case "RTY_2":
                    variables['RTY_ETH_VWAP'] = float(data.loc[2, '[ID7.SG1] ETH_VWAP'])
                    variables['RTY_ETH_TOP_1'] = float(data.loc[2, '[ID7.SG2] Top_1'])
                    variables['RTY_ETH_BOTTOM_1'] = float(data.loc[2, '[ID7.SG3] Bottom_1'])
                    variables['RTY_ETH_TOP_2'] = float(data.loc[2, '[ID7.SG4] Top_2'])
                    variables['RTY_ETH_BOTTOM_2'] = float(data.loc[2, '[ID7.SG5] Bottom_2'])
                    variables['RTY_CPL'] = float(data.loc[2, '[ID2.SG1] CPL'])
                    variables['RTY_5D_VPOC'] = float(data.loc[2, '[ID4.SG2] 5DVPOC'])
                    variables['RTY_20D_VPOC'] = float(data.loc[2, '[ID3.SG2] 20DVPOC'])
                    variables['RTY_P_WOPEN'] = float(data.loc[2, '[ID5.SG1] P_WOPEN'])
                    variables['RTY_P_WHIGH'] = float(data.loc[2, '[ID5.SG2] P_WHIGH'])
                    variables['RTY_P_WLO'] = float(data.loc[2, '[ID5.SG3] P_WLO'])
                    variables['RTY_P_WCLOSE'] = float(data.loc[2, '[ID5.SG4] P_WCLOSE'])
                    variables['RTY_P_WVPOC'] = float(data.loc[2, '[ID11.SG1] P_WVPOC'])
                    variables['RTY_WVWAP'] = float(data.loc[2, '[ID10.SG1] WVWAP'])
                    variables['RTY_P_MOPEN'] = float(data.loc[2, '[ID8.SG1] P_MOPEN'])
                    variables['RTY_P_MHIGH'] = float(data.loc[2, '[ID8.SG2] P_MHIGH'])
                    variables['RTY_P_MLO'] = float(data.loc[2, '[ID8.SG3] P_MLO'])
                    variables['RTY_P_MCLOSE'] = float(data.loc[2, '[ID8.SG4] P_MCLOSE'])
                    variables['RTY_P_MVPOC'] = float(data.loc[2, '[ID12.SG1] P_MVPOC'])
                    variables['RTY_MVWAP'] = float(data.loc[2, '[ID1.SG1] MVWAP'])
               
                case "RTY_3":
                    variables['RTY_IB_ATR'] = float(data.loc[2, '[ID2.SG1] IB ATR'])
                    variables['RTY_IB_HIGH'] = float(data.loc[1, '[ID1.SG2] IBH'])
                    variables['RTY_IB_LOW'] = float(data.loc[1, '[ID1.SG3] IBL'])
               
                case "RTY_4":
                    variables['RTY_OVNH'] = float(data.loc[2, '[ID1.SG2] OVN H'])
                    variables['RTY_OVNL'] = float(data.loc[2, '[ID1.SG3] OVN L'])
                    variables['RTY_TOTAL_OVN_DELTA'] = float(data.loc[2, '[ID3.SG4] OVN Total'])
                
                case "RTY_5":
                    variables['RTY_OVNTOIB_HI'] = float(data.loc[1, '[ID1.SG2] OVNTOIB_HI'])
                    variables['RTY_OVNTOIB_LO'] = float(data.loc[1, '[ID1.SG3] OVNTOIB_LO'])
                
                case "RTY_6":
                    variables['RTY_EURO_IBH'] = float(data.loc[1, '[ID1.SG2] EURO IBH'])
                    variables['RTY_EURO_IBL'] = float(data.loc[1, '[ID1.SG3] EURO IBL'])
                
                case "RTY_7":
                    variables['RTY_ORH'] = float(data.loc[1, '[ID1.SG2] ORH'])
                    variables['RTY_ORL'] = float(data.loc[1, '[ID1.SG3] ORL'])
                
                case "CL_1":
                    variables['CL_DAY_OPEN'] = float(data.loc[2, '[ID2.SG1] Day_Open'])
                    variables['CL_DAY_HIGH'] = float(data.loc[2, '[ID2.SG2] Day_High'])
                    variables['CL_DAY_LOW'] = float(data.loc[2, '[ID2.SG3] Day_Low'])
                    variables['CL_DAY_CLOSE'] = float(data.loc[2, '[ID2.SG4] Day_Close'])
                    variables['CL_DAY_VPOC'] = float(data.loc[2, '[ID1.SG1] Day_Vpoc'])
                    variables['CL_PRIOR_VPOC'] = float(data.loc[2, '[ID9.SG1] Prior_Vpoc'])
                    variables['CL_PRIOR_HIGH'] = float(data.loc[2, '[ID8.SG2] Prior_High'])
                    variables['CL_PRIOR_LOW'] = float(data.loc[2, '[ID8.SG3] Prior_Low'])
                    variables['CL_PRIOR_CLOSE'] = float(data.loc[2, '[ID8.SG4] Prior_Close'])
                    variables['CL_RVOL'] = float(data.loc[2, '[ID6.SG1] R_Vol'])
                    variables['CL_CUMULATIVE_RVOL'] = float(data.loc[2, '[ID6.SG2] R_Vol_Cumulative'])
                    variables['CL_TOTAL_RTH_DELTA'] = float(data.loc[2, '[ID4.SG4] Total_Delta'])

                    # FIGURE OUT A WAY TO MAKE THESE COSISTENT AND LOCATABLE
                    variables['CL_A_HIGH'] = float(data.loc[2, '[ID10.SG1] A_High'])
                    variables['CL_A_LOW'] = float(data.loc[2, '[ID10.SG2] A_Low'])
                    variables['CL_B_HIGH'] = float(data.loc[2, '[ID11.SG1] B_High'])
                    variables['CL_B_LOW'] = float(data.loc[2, '[ID11.SG2] B_Low'])
                    variables['CL_C_HIGH'] = float(data.loc[2, '[ID12.SG1] C_High'])
                    variables['CL_C_LOW'] = float(data.loc[2, '[ID12.SG2] C_Low'])
                    variables['CL_D_HIGH'] = float(data.loc[2, '[ID13.SG1] D_High'])
                    variables['CL_D_LOW'] = float(data.loc[2, '[ID13.SG2] D_Low'])
                    variables['CL_E_HIGH'] = float(data.loc[2, '[ID14.SG1] E_High'])
                    variables['CL_E_LOW'] = float(data.loc[2, '[ID14.SG2] E_Low'])
                    variables['CL_F_HIGH'] = float(data.loc[2, '[ID15.SG1] F_High'])
                    variables['CL_F_LOW'] = float(data.loc[2, '[ID15.SG2] F_Low'])
                    variables['CL_G_HIGH'] = float(data.loc[2, '[ID16.SG1] G_High'])
                    variables['CL_G_LOW'] = float(data.loc[2, '[ID16.SG2] G_Low'])
                    variables['CL_H_HIGH'] = float(data.loc[2, '[ID17.SG1] H_High'])
                    variables['CL_H_LOW'] = float(data.loc[2, '[ID17.SG2] H_Low'])
                    variables['CL_I_HIGH'] = float(data.loc[2, '[ID18.SG1] I_High'])
                    variables['CL_I_LOW'] = float(data.loc[2, '[ID18.SG2] I_Low'])
                    variables['CL_J_HIGH'] = float(data.loc[2, '[ID19.SG1] J_High'])
                    variables['CL_J_LOW'] = float(data.loc[2, '[ID19.SG2] J_Low'])                
                    variables['CL_K_HIGH'] = float(data.loc[2, '[ID20.SG1] K_High'])
                    variables['CL_K_LOW'] = float(data.loc[2, '[ID20.SG2] K_Low'])                              
                
                case "CL_2":
                    variables['CL_ETH_VWAP'] = float(data.loc[2, '[ID7.SG1] ETH_VWAP'])
                    variables['CL_ETH_TOP_1'] = float(data.loc[2, '[ID7.SG2] Top_1'])
                    variables['CL_ETH_BOTTOM_1'] = float(data.loc[2, '[ID7.SG3] Bottom_1'])
                    variables['CL_ETH_TOP_2'] = float(data.loc[2, '[ID7.SG4] Top_2'])
                    variables['CL_ETH_BOTTOM_2'] = float(data.loc[2, '[ID7.SG5] Bottom_2'])
                    variables['CL_CPL'] = float(data.loc[2, '[ID2.SG1] CPL'])
                    variables['CL_5D_VPOC'] = float(data.loc[2, '[ID4.SG2] 5DVPOC'])
                    variables['CL_20D_VPOC'] = float(data.loc[2, '[ID3.SG2] 20DVPOC'])
                    variables['CL_P_WOPEN'] = float(data.loc[2, '[ID5.SG1] P_WOPEN'])
                    variables['CL_P_WHIGH'] = float(data.loc[2, '[ID5.SG2] P_WHIGH'])
                    variables['CL_P_WLO'] = float(data.loc[2, '[ID5.SG3] P_WLO'])
                    variables['CL_P_WCLOSE'] = float(data.loc[2, '[ID5.SG4] P_WCLOSE'])
                    variables['CL_P_WVPOC'] = float(data.loc[2, '[ID11.SG1] P_WVPOC'])
                    variables['CL_WVWAP'] = float(data.loc[2, '[ID10.SG1] WVWAP'])
                    variables['CL_P_MOPEN'] = float(data.loc[2, '[ID8.SG1] P_MOPEN'])
                    variables['CL_P_MHIGH'] = float(data.loc[2, '[ID8.SG2] P_MHIGH'])
                    variables['CL_P_MLO'] = float(data.loc[2, '[ID8.SG3] P_MLO'])
                    variables['CL_P_MCLOSE'] = float(data.loc[2, '[ID8.SG4] P_MCLOSE'])
                    variables['CL_P_MVPOC'] = float(data.loc[2, '[ID12.SG1] P_MVPOC'])
                    variables['CL_MVWAP'] = float(data.loc[2, '[ID1.SG1] MVWAP'])
                
                case "CL_3":
                    variables['CL_IB_ATR'] = float(data.loc[2, '[ID2.SG1] IB ATR'])
                    variables['CL_IB_HIGH'] = float(data.loc[1, '[ID1.SG2] IBH'])
                    variables['CL_IB_LOW'] = float(data.loc[1, '[ID1.SG3] IBL'])
                
                case "CL_4":
                    variables['CL_OVNH'] = float(data.loc[2, '[ID1.SG2] OVN H'])
                    variables['CL_OVNL'] = float(data.loc[2, '[ID1.SG3] OVN L'])
                    variables['CL_TOTAL_OVN_DELTA'] = float(data.loc[2, '[ID3.SG4] OVN Total'])
                
                case "CL_5":
                    variables['CL_OVNTOIB_HI'] = float(data.loc[1, '[ID1.SG2] OVNTOIB_HI'])
                    variables['CL_OVNTOIB_LO'] = float(data.loc[1, '[ID1.SG3] OVNTOIB_LO'])
                
                case "CL_6":
                    variables['CL_EURO_IBH'] = float(data.loc[1, '[ID1.SG2] EURO IBH'])
                    variables['CL_EURO_IBL'] = float(data.loc[1, '[ID1.SG3] EURO IBL'])

                case "CL_7":
                    variables['CL_ORH'] = float(data.loc[1, '[ID1.SG2] ORH'])
                    variables['CL_ORL'] = float(data.loc[1, '[ID1.SG3] ORL'])
                
                case other:
                    print("Case Does Not Exist!")
                
                    
            product_name = task["name"].split('_')[0]

            # Initialize product's variables dictionary if not already present
            if product_name not in all_variables:
                all_variables[product_name] = {}

            # Update the product's variables
            all_variables[product_name].update(variables)  
            
            logger.debug(f"Extracted variables for task '{task['name']}': {variables}")
            
        return all_variables
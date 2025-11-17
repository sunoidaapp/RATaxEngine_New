package vision;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Optional;

public class TAXEngineProcess {
	
	String dynamicProgramName = "taxEngine";
	String dataBaseType = System.getenv("RA_DATABASE_TYPE");
	String tablePrefix = isValid(System.getenv("RA_TABLE_PREFIX"))?System.getenv("RA_TABLE_PREFIX"):"";
	
	String jdbcUrlMain = System.getenv("VISION_APP_CONNECT_STRING");
	String username = System.getenv("VISION_USER_NAME");
	String passwordEncryptFlag = isValid(System.getenv("VISION_PASSWORD_ENCRYPT_FLAG"))?System.getenv("VISION_PASSWORD_ENCRYPT_FLAG"):"N";
	String password = System.getenv("VISION_PASSWORD");
	
	Connection connectionMain = null;
	Statement stmt = null;
	ResultSet rs;
	FileWriter logfile = null;
	BufferedWriter bufferedWriter = null;
	
	int ERROR_OPERATION = 1;
	int SUCCESS_OPERATION = 0;
	int DB_CONNECTION_ERROR = 2;
	int finalReturnValue;
	DateFormat dateTimeFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
	
	String countryObje;
	String leBookObje;  
	String businessDateObje; 
	String businessLineIdObje;
	String debugModeObje;
	String logPathObje;
	
	String businessDateObje_YYYYMMDD;
	String businessDateObje_YYYYMM;
	String businessDateObje_DD;
	String localDateTime;
	
	String  P_Trans_Line_Type;
	
	DateTimeFormatter dtf_local = DateTimeFormatter.ofPattern("dd-MMM-yyyy HH:mm:ss");
	
	public int doTaxActivityProcess(String country, String leBook, String businessDate, String businessLineId, String debugMode, String logPath){
		
		
		ExceptionCode exceptionCode = new ExceptionCode();
		Date programStartDateTime = new Date();
	
		
		try {
			
			
			countryObje = country;
			leBookObje = leBook;
			businessDateObje = businessDate;
			businessLineIdObje = businessLineId;
			logPathObje = logPath;
			
			if("Y".equalsIgnoreCase(debugMode)) {
				debugModeObje = "Y";
			} else {
				debugModeObje = "N";
			}
			
			int retVal, recordCount;
			String strQuery;
			
			String P_TableName;
			String P_Target_TableName;
			String P_CCY_ColName;
			String P_TranCount_ColName;
			String P_Sub_TranCount_ColName;
			
			DateTimeFormatter dtf1 = new DateTimeFormatterBuilder().parseCaseInsensitive().appendPattern("dd-MMM-yyyy").toFormatter();
		    DateTimeFormatter dtf2 = DateTimeFormatter.ofPattern("yyyyMMdd");
		    
		    businessDateObje_YYYYMMDD = LocalDate.parse(businessDateObje, dtf1).format(dtf2);
		    
		    logWriter("Process Started...");
		    logWriter(String.format("Parameters <country:%s> <le_book:%s> <business_date:%s> <business_line_id:%s> <debug_mode:%s> <log_path_with_file_name:%s>"
		    		,countryObje,leBookObje,businessDateObje,businessLineIdObje,debugModeObje,logPathObje));
		    
		    //System.out.printf("%s%n", businessDateObje_YYYYMMDD);
		    
			
		    retVal = connnectToDatabase(jdbcUrlMain, username, password, dataBaseType);
			
			if(retVal != SUCCESS_OPERATION){
				finalReturnValue = ERROR_OPERATION;
		    	return ERROR_OPERATION;
		    }

			stmt = connectionMain.createStatement();
			
			
			exceptionCode = getVisionVarValue();
			
			if(exceptionCode.getErrorCode() != SUCCESS_OPERATION) {
				logWriter(exceptionCode.getErrorMsg());
				finalReturnValue = ERROR_OPERATION;
				return ERROR_OPERATION;
			}
			   
			String  P_Vision_Var_Check = (String)exceptionCode.getResponse();
			
			if(!isValid(P_Vision_Var_Check)){
				logWriter("Vision Variable RA_TAX_MODULE_COMPUTE_FLAG value is invalid !!");
				finalReturnValue = ERROR_OPERATION;
				return ERROR_OPERATION;
			}
			if("N".equalsIgnoreCase(P_Vision_Var_Check)) {
				logWriter("Vision Variable RA_TAX_MODULE_COMPUTE_FLAG Flag is N !!");
				finalReturnValue = SUCCESS_OPERATION;
				return SUCCESS_OPERATION;
			}
			
			
			exceptionCode = getTransLineType();
			
			if(exceptionCode.getErrorCode() != SUCCESS_OPERATION) {
				logWriter(exceptionCode.getErrorMsg());
				finalReturnValue = ERROR_OPERATION;
				return ERROR_OPERATION;
			}
			
			P_Trans_Line_Type = (String)exceptionCode.getResponse();
			
			if(!isValid(P_Trans_Line_Type)){
				logWriter("Invalid Trans Line Type value !!");
				finalReturnValue = ERROR_OPERATION;
				return ERROR_OPERATION;
			}

			
			strQuery = new String("drop table "+tablePrefix+"RA_MST_TE_"+countryObje+leBookObje+businessDateObje_YYYYMMDD+"_"+businessLineIdObje+" ");
			
			retVal = executeStatment(strQuery, false, "");
				
			
			if("ORACLE".equalsIgnoreCase(dataBaseType)) {
				strQuery = new String(
								 "Create Table "+tablePrefix+"RA_MST_TE_"+countryObje+leBookObje+businessDateObje_YYYYMMDD+"_"+businessLineIdObje+" As "
								+"     Select T1.*,T2.BUSINESS_LINE_ID  "
								+"     From "+tablePrefix+"RA_Mst_Tax_Expanded T1, "+tablePrefix+"RA_Mst_BL_Tax_Link T2 "
								+"     WHERE T1.COUNTRY = T2.COUNTRY "
								+"       AND T1.LE_BOOK = T2.LE_BOOK "
								+"       AND T1.TAX_LINE_ID =  T2.TAX_LINE_ID  "
								+"       And T2.COUNTRY = '"+countryObje+"' "
								+"       AND T2.LE_BOOK = '"+leBookObje+"' "
								+"       AND T2.BUSINESS_LINE_ID = '"+businessLineIdObje+"' "
								+"       AND T2.LINK_STATUS = 0 "
								+"       AND T1.TAX_LINE_STATUS = 0 "
								+"       AND T1.Effective_Date = (Select Max(X.Effective_Date)  "
								+"                                From "+tablePrefix+"RA_Mst_Tax_Expanded X "
								+"                                Where T1.Country = X.Country "
								+"                                  And T1.Le_Book = X.Le_Book "
								+"                                  And T1.Tax_Line_Id = X.Tax_Line_Id "
								+"                                  And T1.Product_ID = X.Product_ID "
								+"                                  And T1.Product_Type = X.Product_Type "
								+"                                  And T1.Tran_ccy = X.Tran_ccy "
								+"                                  And T1.Tier_Sequence = X.Tier_Sequence "
								+"                                  And X.Effective_Date <= TO_DATE('"+businessDateObje+"','DD-MON-YYYY')) "
						);
			}else if("MSSQL".equalsIgnoreCase(dataBaseType)) {
				strQuery = new String(
						 "     Select T1.*,T2.BUSINESS_LINE_ID  "
						+"     Into "+tablePrefix+"RA_MST_TE_"+countryObje+leBookObje+businessDateObje_YYYYMMDD+"_"+businessLineIdObje+" "
						+"     From "+tablePrefix+"RA_Mst_Tax_Expanded T1, "+tablePrefix+"RA_Mst_BL_Tax_Link T2 "
						+"     WHERE T1.COUNTRY = T2.COUNTRY "
						+"       AND T1.LE_BOOK = T2.LE_BOOK "
						+"       AND T1.TAX_LINE_ID =  T2.TAX_LINE_ID  "
						+"       And T2.COUNTRY = '"+countryObje+"' "
						+"       AND T2.LE_BOOK = '"+leBookObje+"' "
						+"       AND T2.BUSINESS_LINE_ID = '"+businessLineIdObje+"' "
						+"       AND T2.LINK_STATUS = 0 "
						+"       AND T1.TAX_LINE_STATUS = 0 "
						+"       AND T1.Effective_Date = (Select Max(X.Effective_Date)  "
						+"                                From "+tablePrefix+"RA_Mst_Tax_Expanded X "
						+"                                Where T1.Country = X.Country "
						+"                                  And T1.Le_Book = X.Le_Book "
						+"                                  And T1.Tax_Line_Id = X.Tax_Line_Id "
						+"                                  And T1.Product_ID = X.Product_ID "
						+"                                  And T1.Product_Type = X.Product_Type "
						+"                                  And T1.Tran_ccy = X.Tran_ccy "
						+"                                  And T1.Tier_Sequence = X.Tier_Sequence "
						+"                                  And X.Effective_Date <= CONVERT(DATE,'"+businessDateObje+"')) "
				);
			}else {
				   logWriter("Configuration for given database type is unavailable !!");
				   finalReturnValue = ERROR_OPERATION;
				   return ERROR_OPERATION;
			}
			
			logWriter("Creating RA_MST_TE_ table for given input parameters..");
			
			if("Y".equals(debugModeObje)) {
				logWriter(strQuery);
			}
			
			retVal = executeStatment(strQuery, true, "CREATEINS");

		    if(retVal != SUCCESS_OPERATION){
		    	logWriter("Error while creating RA_MST_TE_ process table !!");
		    	finalReturnValue = ERROR_OPERATION;
		    	return ERROR_OPERATION;
		    }
				
		    
		    exceptionCode = getCheckTaxRecordCount();
			
			if(exceptionCode.getErrorCode() != SUCCESS_OPERATION) {
				logWriter(exceptionCode.getErrorMsg());
				finalReturnValue = ERROR_OPERATION;
				return ERROR_OPERATION;
			}
			
			recordCount = (Integer)exceptionCode.getResponse();
			
			if(recordCount == 0){
				logWriter("There is no Tax configuration for given Country, Le_book, Business Line, Business Date !!");
				finalReturnValue = SUCCESS_OPERATION;
				return SUCCESS_OPERATION;
			}
			
			strQuery = new String(
					" create unique index RA_MST_TE_"+countryObje+leBookObje+businessDateObje_YYYYMMDD+"_"+businessLineIdObje+"I "
				   +" On "+tablePrefix+"RA_MST_TE_"+countryObje+leBookObje+businessDateObje_YYYYMMDD+"_"+businessLineIdObje
				   +" (COUNTRY, LE_BOOK, BUSINESS_LINE_ID, TAX_LINE_ID, PRODUCT_TYPE, PRODUCT_ID, TRAN_CCY, TIER_SEQUENCE) ");
			
			retVal = executeStatment(strQuery, true, "CREATEINDEX");

		    if(retVal != SUCCESS_OPERATION){
		    	logWriter("Error while creating unique index on RA_MST_TE_ table !!");
		    	finalReturnValue = ERROR_OPERATION;
		    	return ERROR_OPERATION;
		    }
			

		    exceptionCode = getCheckActualRecordCount();
			
			if(exceptionCode.getErrorCode() != SUCCESS_OPERATION) {
				logWriter(exceptionCode.getErrorMsg());
				finalReturnValue = ERROR_OPERATION;
				return ERROR_OPERATION;
			}
			
			recordCount = (Integer)exceptionCode.getResponse();
			
			if(recordCount == 0){
				logWriter("There is no Activities available for given Country, Le_book, Business Line, Business Date !!");
				finalReturnValue = SUCCESS_OPERATION;
				return SUCCESS_OPERATION;
			}
			
			if(P_Trans_Line_Type.equalsIgnoreCase("S")) {
				P_TableName         = "RA_SERVICE_IE_TRANSACTIONS";
				P_Target_TableName  = "RA_SERVICE_TAX_TRANSACTIONS";
				P_CCY_ColName       = "T1.TRN_CCY";
				P_TranCount_ColName = "T1.TRN_COUNT";
				
				
				if("ORACLE".equalsIgnoreCase(dataBaseType)) {
					
					P_Sub_TranCount_ColName = "NVL(T1.SUB_TRN_COUNT,1)";
					
				}else if("MSSQL".equalsIgnoreCase(dataBaseType)) {
					
					P_Sub_TranCount_ColName = "ISNULL(T1.SUB_TRN_COUNT,1)";
					
				}else {
					   logWriter("Configuration for given database type is unavailable !!");
					   finalReturnValue = ERROR_OPERATION;
					   return ERROR_OPERATION;
				}
			}
			else {
				P_TableName         = "RA_PRODUCT_IE_TRANSACTIONS";
				P_Target_TableName  = "RA_PRODUCT_TAX_TRANSACTIONS";
				P_CCY_ColName       = "T1.CONTRACT_CCY";
				
				if("ORACLE".equalsIgnoreCase(dataBaseType)) {
					
					P_TranCount_ColName = "1";
					P_Sub_TranCount_ColName = "1";
					
				}else if("MSSQL".equalsIgnoreCase(dataBaseType)) {
					
					P_TranCount_ColName = "cast(1 as float)";
					P_Sub_TranCount_ColName = "cast(1 as float)";
					
				}else {
					   logWriter("Configuration for given database type is unavailable !!");
					   finalReturnValue = ERROR_OPERATION;
					   return ERROR_OPERATION;
				}
				
			}
				
			StringBuilder strQuery_SB = new StringBuilder();
			strQuery_SB.append(
					          "delete from "+tablePrefix+""+P_Target_TableName+" "
					        + " WHERE COUNTRY = '"+countryObje+"' "
					 		+ "AND LE_BOOK = '"+leBookObje+"' "
					 		+ "AND BUSINESS_LINE_ID = '"+businessLineIdObje+"' "
			);
			
			if("ORACLE".equalsIgnoreCase(dataBaseType)) {
				
				strQuery_SB.append("       AND BUSINESS_DATE = TO_DATE('"+businessDateObje+"','DD-MON-YYYY') ");
				
			}else if("MSSQL".equalsIgnoreCase(dataBaseType)) {
				
				strQuery_SB.append("       AND BUSINESS_DATE = convert(date,'"+businessDateObje+"') ");
				
			}else {
				   logWriter("Configuration for given database type is unavailable !!");
				   finalReturnValue = ERROR_OPERATION;
				   return ERROR_OPERATION;
			}
			
			
			logWriter("Deleting from "+tablePrefix+P_Target_TableName+" table for given parameters..");
			
			if("Y".equals(debugModeObje)) {
				logWriter(strQuery_SB.toString());
			}
			
		   retVal = executeStatment(strQuery_SB.toString(), true, "DELETE");
		
		   if(retVal != SUCCESS_OPERATION){
			    logWriter("Error while deleting from "+tablePrefix+""+P_Target_TableName+" table !!");
			    finalReturnValue = ERROR_OPERATION;
				return ERROR_OPERATION;
		   }
		   
		   //List<TAXEngineSourceTabColVb> SOURCE_MAPPING_TABLE = getSourceMappingTable();
		   
		   exceptionCode = getSourceMappingTable();
		   
		   if(exceptionCode.getErrorCode() != SUCCESS_OPERATION) {
			    logWriter(exceptionCode.getErrorMsg());
			    finalReturnValue = ERROR_OPERATION;
				return ERROR_OPERATION;
		   }
		   
		   List<TAXEngineSourceTabColVb> SOURCE_MAPPING_TABLE = (List<TAXEngineSourceTabColVb>)exceptionCode.getResponse();
		   
		   if(SOURCE_MAPPING_TABLE == null || SOURCE_MAPPING_TABLE.isEmpty()){
			    finalReturnValue = ERROR_OPERATION;
				return ERROR_OPERATION;
		   }

			//List<TAXEngineTaxIDColVb> TAX_ID_TABLE = getTAXIDTable();
			
			exceptionCode = getTAXIDTable();
			
			if(exceptionCode.getErrorCode() != SUCCESS_OPERATION) {
				logWriter(exceptionCode.getErrorMsg());
				finalReturnValue = ERROR_OPERATION;
				return ERROR_OPERATION;
		   }
			
			List<TAXEngineTaxIDColVb> TAX_ID_TABLE = (List<TAXEngineTaxIDColVb>)exceptionCode.getResponse();
			if(TAX_ID_TABLE == null || TAX_ID_TABLE.isEmpty()){
				finalReturnValue = ERROR_OPERATION;
				return ERROR_OPERATION;
			}
			
			
			for(int ind_i = 0; ind_i<TAX_ID_TABLE.size() ; ind_i++){
				
				StringBuilder SQLTableList = new StringBuilder();
				StringBuilder SQLFilterConditions = new StringBuilder();
				String columnAlias = "";
				
				TAXEngineTaxIDColVb taxIDColVb = TAX_ID_TABLE.get(ind_i);
				
				logWriter("Processing For Tax Line Id ("+taxIDColVb.getTaxLineId()+") !!");

				if(taxIDColVb.getFilterTableListFlag().equalsIgnoreCase("Y")) {
					
					//List<TAXEngineMappingTablVb> EXL_FILTER_TABLE = getEXLFilterTable(taxIDColVb.getTaxLineId());
					
					exceptionCode = getEXLFilterTable(taxIDColVb.getTaxLineId());
					
					if(exceptionCode.getErrorCode() != SUCCESS_OPERATION) {
						logWriter(exceptionCode.getErrorMsg());
						finalReturnValue = ERROR_OPERATION;
						return ERROR_OPERATION;
				   }
					
					
					List<TAXEngineMappingTablVb> EXL_FILTER_TABLE = (List<TAXEngineMappingTablVb>)exceptionCode.getResponse();
					
					if(EXL_FILTER_TABLE == null || EXL_FILTER_TABLE.isEmpty()){
						finalReturnValue = ERROR_OPERATION;
						return ERROR_OPERATION;
					}
					
					for(int ind_j = 0; ind_j<EXL_FILTER_TABLE.size() ; ind_j++){
						
						TAXEngineMappingTablVb mappingTabVb = EXL_FILTER_TABLE.get(ind_j);
						
						/* Finding Source Table to Join */
						Optional<TAXEngineSourceTabColVb> srcMappingTabVbOpt = 
								SOURCE_MAPPING_TABLE.stream()
								.filter(srcMappingTabVb1 -> srcMappingTabVb1.getTableName().equals(mappingTabVb.getFilterTable()))
								.findFirst();
						
						if(srcMappingTabVbOpt.isPresent()) {
							
							TAXEngineSourceTabColVb srcMappingTabVb = srcMappingTabVbOpt.get();
							
							columnAlias = srcMappingTabVb.getTableAlias();
							
							if(srcMappingTabVb.getJoinStatus().equalsIgnoreCase("N")) {

								srcMappingTabVb.setJoinStatus("Y");
								
								SQLTableList.append(" "+srcMappingTabVb.getJoinType());
								SQLTableList.append(" "+srcMappingTabVb.getSourceTable());
								SQLTableList.append(" "+srcMappingTabVb.getTableAlias());
								
								SQLTableList.append(" ON ("+genericReplaceValues(srcMappingTabVb.getGenericCondition())+")");
								
							}
							
						}
						else {
							logWriter("Source Mapping Table unavailable for "+mappingTabVb.getFilterTable()+" table !!");
							finalReturnValue = ERROR_OPERATION;
							return ERROR_OPERATION;
						}
						/* Finding Source Table to Join End */
						/* Adding Filters */
						
						String conditionValue1 = genericReplaceValues(mappingTabVb.getConditionValue1()); 
						mappingTabVb.setConditionValue1(conditionValue1);
					
				        String conditionValue2 = genericReplaceValues(mappingTabVb.getConditionValue2()); 
				        mappingTabVb.setConditionValue2(conditionValue2);
				        
				        SQLFilterConditions.append(" And ");
				        
				        if(mappingTabVb.getConditionOperation().toUpperCase().contains("IS NULL")
				        		|| mappingTabVb.getConditionOperation().toUpperCase().contains("IS NOT NULL")){// in ('IS NULL','IS NOT NULL'))
				        	SQLFilterConditions.append(" (");
				        	SQLFilterConditions.append(columnAlias+"."+mappingTabVb.getFilterColumn()+" "+mappingTabVb.getConditionOperation());
				        	SQLFilterConditions.append(")");
				        	//SQLFilterConditions = SQLFilterConditions +" ("+columnAlias+"."+mappingTabVb.getFilterColumn()+" "+mappingTabVb.getConditionOperation()+")";
				        }else if(mappingTabVb.getConditionOperation().toUpperCase().contains("IN")
				        		|| mappingTabVb.getConditionOperation().toUpperCase().contains("NOT IN")){// in ('IN','NOT IN'))
				            //SQLFilterConditions = SQLFilterConditions+" ("+columnAlias+"."+mappingTabVb.getFilterColumn()+" "+mappingTabVb.getConditionOperation()+" ("+mappingTabVb.getConditionValue1()")";
				        	SQLFilterConditions.append(" (");
				        	SQLFilterConditions.append(columnAlias+"."+mappingTabVb.getFilterColumn()+" "+mappingTabVb.getConditionOperation()+" ("+mappingTabVb.getConditionValue1());
				        	SQLFilterConditions.append(")");
				        }else if(mappingTabVb.getConditionOperation().toUpperCase().contains("BETWEEN") 
				        		|| mappingTabVb.getConditionOperation().toUpperCase().contains("NOT BETWEEN")){ // ('BETWEEN','NOT BETWEEN'))
				        	SQLFilterConditions.append(" (");
				        	SQLFilterConditions.append(columnAlias+"."+mappingTabVb.getFilterColumn()+" "+mappingTabVb.getConditionOperation());
				        	SQLFilterConditions.append(" "+mappingTabVb.getConditionValue1()+" AND "+mappingTabVb.getConditionValue2());
				        	SQLFilterConditions.append(")");
				        }else if(mappingTabVb.getConditionOperation().contains("EXPRESSION")){ //('EXPRESSION'))
				        	SQLFilterConditions.append(" (");
				        	SQLFilterConditions.append(" "+mappingTabVb.getConditionValue1()+"  "+mappingTabVb.getConditionValue2());
				        	SQLFilterConditions.append(")");
				        }else{ /* = | != | Like | Not Like | etc.*/
				        	SQLFilterConditions.append(" (");
				        	SQLFilterConditions.append(columnAlias+"."+mappingTabVb.getFilterColumn()+" "+mappingTabVb.getConditionOperation());
				        	SQLFilterConditions.append(" "+mappingTabVb.getConditionValue1());
				        	SQLFilterConditions.append(")");
				        }
					}
				}
				
				logWriter("SQLTableList:"+SQLTableList+"!!");
				logWriter("SQLFilterConditions:"+SQLFilterConditions+"!!");
				
				strQuery_SB = new StringBuilder();
				strQuery_SB.append(
						         "  Insert Into "+tablePrefix+""+P_Target_TableName+" "
						        +"(country,le_book,business_date,trans_line_id,trans_sequence,"
						        +" business_line_id,tax_line_id_at,tax_line_id,Expected_Tax_Amt_Fcy,Expected_Tax_Amt_Lcy, "
						        +" PL_GL,Office_Account,Tax_Amt,Tax_Percentage,Date_Creation) "
						        +"  SELECT  "
						        +"      T1.Country "
						        +"     ,T1.Le_Book "
						        +"     ,T1.Business_Date "
						        +"     ,T1.Trans_Line_ID "
						        +"     ,T1.Trans_Sequence "
						        +"     ,T1.Business_Line_ID "
						        +"     ,7099 TAX_LINE_ID_AT "
						        +"     ,T2.Tax_Line_ID "
						        +"     ,0 Expected_Tax_Amt_Fcy "
						        +"     , Sum( "
						        +"       Case When T2.Tax_Charge_Type = 'F' And T2.Tier_Type = 'NA' And T2.Tax_Basis = 'U' Then ("+P_TranCount_ColName+" * T2.TAX_AMT) "
						        
						        +"            When T2.Tax_Charge_Type = 'F' And T2.Tier_Type = 'NA' And T2.Tax_Basis = 'UX' Then ("+P_TranCount_ColName+" * "+P_Sub_TranCount_ColName+" * T2.TAX_AMT) "
						        
						        +"            When T2.Tax_Charge_Type = 'F' And T2.Tier_Type = 'NA' And T2.Tax_Basis = 'P' Then (CASE WHEN T2.LOOKUP_AMOUNT_TYPE = 'A' THEN T1.ACTUAL_IE_AMT_LCY ELSE T1.EXPECTED_IE_AMT_LCY END * (T2.Tax_Percentage/100)) "
						        +"            When T2.Tax_Charge_Type = 'R' And T2.Tier_Type = 'A'  And T2.Tax_Basis = 'U' Then "
						        +"                (Case When (CASE WHEN T2.LOOKUP_AMOUNT_TYPE = 'A' THEN T1.ACTUAL_IE_AMT_LCY ELSE T1.EXPECTED_IE_AMT_LCY END) between T2.AMT_FROM and T2.AMT_TO Then ("+P_TranCount_ColName+" * T2.TAX_AMT) Else 0 End) "
						        
						        +"            When T2.Tax_Charge_Type = 'R' And T2.Tier_Type = 'A'  And T2.Tax_Basis = 'UX' Then "
						        +"                (Case When (CASE WHEN T2.LOOKUP_AMOUNT_TYPE = 'A' THEN T1.ACTUAL_IE_AMT_LCY ELSE T1.EXPECTED_IE_AMT_LCY END) between T2.AMT_FROM and T2.AMT_TO Then ("+P_TranCount_ColName+" * "+P_Sub_TranCount_ColName+" * T2.TAX_AMT) Else 0 End) "
						        
						        +"            When T2.Tax_Charge_Type = 'R' And T2.Tier_Type = 'A'  And T2.Tax_Basis = 'P' Then "
						        +"                (Case When (CASE WHEN T2.LOOKUP_AMOUNT_TYPE = 'A' THEN T1.ACTUAL_IE_AMT_LCY ELSE T1.EXPECTED_IE_AMT_LCY END) between T2.AMT_FROM and T2.AMT_TO Then (CASE WHEN T2.LOOKUP_AMOUNT_TYPE = 'A' THEN T1.ACTUAL_IE_AMT_LCY ELSE T1.EXPECTED_IE_AMT_LCY END * (T2.Tax_Percentage/100)) Else 0 End) "
						        +"            When T2.Tax_Charge_Type = 'R' And T2.Tier_Type = 'C'  And T2.Tax_Basis = 'U' Then "
						        +"                (Case When "+P_TranCount_ColName+" between T2.CNT_FROM and T2.CNT_TO Then ("+P_TranCount_ColName+" * T2.TAX_AMT) Else 0 End) "
						        
						        +"            When T2.Tax_Charge_Type = 'R' And T2.Tier_Type = 'C'  And T2.Tax_Basis = 'UX' Then "
						        +"                (Case When ("+P_TranCount_ColName+" * "+P_Sub_TranCount_ColName+") between T2.CNT_FROM and T2.CNT_TO Then ("+P_TranCount_ColName+" * "+P_Sub_TranCount_ColName+" * T2.TAX_AMT) Else 0 End) "
						        
						        +"            When T2.Tax_Charge_Type = 'R' And T2.Tier_Type = 'C'  And T2.Tax_Basis = 'P' Then "
						        +"                (Case When "+P_TranCount_ColName+" between T2.CNT_FROM and T2.CNT_TO Then (CASE WHEN T2.LOOKUP_AMOUNT_TYPE = 'A' THEN T1.ACTUAL_IE_AMT_LCY ELSE T1.EXPECTED_IE_AMT_LCY END * (T2.Tax_Percentage/100)) Else 0 End) "
						        +"            When T2.Tax_Charge_Type = 'T' And T2.Tier_Type = 'A' And T2.Tax_Basis = 'P' Then "
						        +"                (CASE  "
						        +"                    WHEN (CASE WHEN T2.LOOKUP_AMOUNT_TYPE = 'A' THEN T1.ACTUAL_IE_AMT_LCY ELSE T1.EXPECTED_IE_AMT_LCY END)  > T2.AMT_FROM AND (CASE WHEN T2.LOOKUP_AMOUNT_TYPE = 'A' THEN T1.ACTUAL_IE_AMT_LCY ELSE T1.EXPECTED_IE_AMT_LCY END)  > T2.AMT_TO THEN ((T2.AMT_TO-T2.AMT_FROM+1) * (T2.Tax_Percentage/100)) "
						        +"                    WHEN (CASE WHEN T2.LOOKUP_AMOUNT_TYPE = 'A' THEN T1.ACTUAL_IE_AMT_LCY ELSE T1.EXPECTED_IE_AMT_LCY END)  < T2.AMT_FROM AND (CASE WHEN T2.LOOKUP_AMOUNT_TYPE = 'A' THEN T1.ACTUAL_IE_AMT_LCY ELSE T1.EXPECTED_IE_AMT_LCY END)  < T2.AMT_TO THEN 0 "
						        +"                    WHEN (CASE WHEN T2.LOOKUP_AMOUNT_TYPE = 'A' THEN T1.ACTUAL_IE_AMT_LCY ELSE T1.EXPECTED_IE_AMT_LCY END) >= T2.AMT_FROM AND (CASE WHEN T2.LOOKUP_AMOUNT_TYPE = 'A' THEN T1.ACTUAL_IE_AMT_LCY ELSE T1.EXPECTED_IE_AMT_LCY END) <= T2.AMT_TO AND T2.TIER_SEQUENCE>1 THEN (((CASE WHEN T2.LOOKUP_AMOUNT_TYPE = 'A' THEN T1.ACTUAL_IE_AMT_LCY ELSE T1.EXPECTED_IE_AMT_LCY END)-T2.AMT_FROM+1) * (T2.Tax_Percentage/100))  "
						        +"                    WHEN (CASE WHEN T2.LOOKUP_AMOUNT_TYPE = 'A' THEN T1.ACTUAL_IE_AMT_LCY ELSE T1.EXPECTED_IE_AMT_LCY END) >= T2.AMT_FROM AND (CASE WHEN T2.LOOKUP_AMOUNT_TYPE = 'A' THEN T1.ACTUAL_IE_AMT_LCY ELSE T1.EXPECTED_IE_AMT_LCY END) <= T2.AMT_TO AND T2.TIER_SEQUENCE=1 THEN ((CASE WHEN T2.LOOKUP_AMOUNT_TYPE = 'A' THEN T1.ACTUAL_IE_AMT_LCY ELSE T1.EXPECTED_IE_AMT_LCY END) * (T2.Tax_Percentage/100)) "
						        +"                ELSE 0 END) "
						        +"            When T2.Tax_Charge_Type = 'T' And T2.Tier_Type = 'C' And T2.Tax_Basis = 'U' Then "
						        +"                (CASE  "
						        +"                    WHEN "+P_TranCount_ColName+"  > T2.CNT_FROM AND "+P_TranCount_ColName+"  > T2.CNT_TO THEN ((T2.CNT_TO-T2.CNT_FROM+1) * T2.TAX_AMT) "
						        +"                    WHEN "+P_TranCount_ColName+"  < T2.CNT_FROM AND "+P_TranCount_ColName+"  < T2.CNT_TO THEN 0 "
						        +"                    WHEN "+P_TranCount_ColName+" >= T2.CNT_FROM AND "+P_TranCount_ColName+" <= T2.CNT_TO AND T2.TIER_SEQUENCE>1 THEN (("+P_TranCount_ColName+"-T2.CNT_FROM+1) * T2.TAX_AMT)  "
						        +"                    WHEN "+P_TranCount_ColName+" >= T2.CNT_FROM AND "+P_TranCount_ColName+" <= T2.CNT_TO AND T2.TIER_SEQUENCE=1 THEN (("+P_TranCount_ColName+") * T2.TAX_AMT) "
						        +"                ELSE 0 END) "
						        
								+"            When T2.Tax_Charge_Type = 'T' And T2.Tier_Type = 'C' And T2.Tax_Basis = 'UX' Then "
								+"                (CASE  "
								+"                    WHEN ("+P_TranCount_ColName+" * "+P_Sub_TranCount_ColName+")  > T2.CNT_FROM AND ("+P_TranCount_ColName+" * "+P_Sub_TranCount_ColName+")  > T2.CNT_TO THEN ((T2.CNT_TO-T2.CNT_FROM+1) * T2.TAX_AMT) "
								+"                    WHEN ("+P_TranCount_ColName+" * "+P_Sub_TranCount_ColName+")  < T2.CNT_FROM AND ("+P_TranCount_ColName+" * "+P_Sub_TranCount_ColName+")  < T2.CNT_TO THEN 0 "
								+"                    WHEN ("+P_TranCount_ColName+" * "+P_Sub_TranCount_ColName+") >= T2.CNT_FROM AND ("+P_TranCount_ColName+" * "+P_Sub_TranCount_ColName+") <= T2.CNT_TO AND T2.TIER_SEQUENCE>1 THEN ((("+P_TranCount_ColName+" * "+P_Sub_TranCount_ColName+")-T2.CNT_FROM+1) * T2.TAX_AMT)  "
								+"                    WHEN ("+P_TranCount_ColName+" * "+P_Sub_TranCount_ColName+") >= T2.CNT_FROM AND ("+P_TranCount_ColName+" * "+P_Sub_TranCount_ColName+") <= T2.CNT_TO AND T2.TIER_SEQUENCE=1 THEN ((("+P_TranCount_ColName+" * "+P_Sub_TranCount_ColName+")) * T2.TAX_AMT) "
								+"                ELSE 0 END) "
						        
						        +"       Else 0 End) Expected_Tax_Amt_Lcy "
						        +"     ,T2.PL_GL "
						        +"     ,T2.Office_Account "
						        +"     ,sum(case when T2.Tax_Charge_Type = 'F' Then T2.Tax_Amt else 0 end) Tax_Amt "
						        +"     ,sum(case when T2.Tax_Charge_Type = 'F' Then T2.Tax_Percentage else 0 end) Tax_Percentage ");
						        
					
						        if("ORACLE".equalsIgnoreCase(dataBaseType)) {
									
									strQuery_SB.append("     ,sysdate Date_Creation ");
									
								}else if("MSSQL".equalsIgnoreCase(dataBaseType)) {
									
									strQuery_SB.append("     ,GETDATE() Date_Creation ");
									
								}else {
									strQuery_SB.append(" ***Invalid Database Type*** ");
								}
						        
						        strQuery_SB.append(
						        " FROM "+tablePrefix+""+P_TableName+" T1 "
						        +" inner join "+tablePrefix+"RA_MST_TE_"+countryObje+leBookObje+businessDateObje_YYYYMMDD+"_"+businessLineIdObje+" T2 "
						        +"   ON (T1.COUNTRY = T2.COUNTRY "
						        +"   AND T1.LE_BOOK = T2.LE_BOOK "
						        +"   AND T1.BUSINESS_LINE_ID = T2.BUSINESS_LINE_ID "
						        +"   AND T1.PRODUCT_TYPE = T2.PRODUCT_TYPE "
						        +"   AND T1.PRODUCT_ID = T2.PRODUCT_ID "
						        +"   AND "+P_CCY_ColName+" = T2.TRAN_CCY) "+SQLTableList+" "
						        +" WHERE T1.COUNTRY = '"+countryObje+"' "
						        +"   AND T1.LE_BOOK = '"+leBookObje+"' "
						        +"   AND T1.BUSINESS_LINE_ID = '"+businessLineIdObje+"' ");
						        
						        
						        if("ORACLE".equalsIgnoreCase(dataBaseType)) {
									
									strQuery_SB.append("     AND T1.BUSINESS_DATE = to_date('"+businessDateObje+"','DD-MON-YYYY')  ");
									
								}else if("MSSQL".equalsIgnoreCase(dataBaseType)) {
									
									strQuery_SB.append("     AND T1.BUSINESS_DATE = convert(DATE,'"+businessDateObje+"')  ");
									
								}else {
									strQuery_SB.append(" ***Invalid Database Type*** ");
								}
						        
						        strQuery_SB.append(
						         "   And T2.TAX_LINE_ID = '"+taxIDColVb.getTaxLineId()+"' "+SQLFilterConditions+" "
						        +" Group By "
						        +"      T1.Country "
						        +"     ,T1.Le_Book "
						        +"     ,T1.Business_Date "
						        +"     ,T1.Trans_Line_ID "
						        +"     ,T1.Trans_Sequence "
						        +"     ,T1.Business_Line_ID "
						        +"     ,T2.Tax_Line_ID "
						        +"     ,T2.PL_GL "
						        +"     ,T2.Office_Account "
						
						);
						        
				
		        if("Y".equals(debugModeObje)) {
					logWriter(strQuery_SB.toString());
				}
				
				retVal = executeStatment(strQuery_SB.toString(), true, "INSERT");
				
			    if(retVal != SUCCESS_OPERATION){
			    	logWriter("Error while inserting computing Tax for "+taxIDColVb.getTaxLineId()+" !!");
			    	finalReturnValue = ERROR_OPERATION;
					return ERROR_OPERATION;
			    }
				
			}
			
			finalReturnValue = SUCCESS_OPERATION;
			return SUCCESS_OPERATION;
				
		}catch (ClassNotFoundException e) {
			e.printStackTrace();
			finalReturnValue = ERROR_OPERATION;
			return ERROR_OPERATION;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			finalReturnValue = ERROR_OPERATION;
			return ERROR_OPERATION;
		}finally{
			
			Date programEndDateTime = new Date();
			
			long elapsedSeconds = Math.round((double)(programEndDateTime.getTime() - programStartDateTime.getTime()) / 1000);
			
			long localHours,localMinutes,localSeconds,remSeconds;

			localHours    = (elapsedSeconds / (60 * 60));
			remSeconds    = elapsedSeconds - (localHours * (60 * 60));
			localMinutes  = remSeconds / 60 ;
			remSeconds    = remSeconds - (localMinutes * 60);
			localSeconds  = remSeconds;
			
			String finalElapsedString = String.format("Start Time:[%s], End Time:[%s], Difference:[%02d:%02d:%02d]"
					, dateTimeFormat.format(programStartDateTime), dateTimeFormat.format(programEndDateTime), localHours,localMinutes,localSeconds);
			
			
			
			if(finalReturnValue ==  SUCCESS_OPERATION)
			{
				logWriter("Build Completed..."+finalElapsedString);
			} else {
				logWriter("Build Aborted..."+finalElapsedString);
			}
			
			doFinishingSteps();
		}
	}
	
	private String genericReplaceValues(String value){
		if(value.indexOf("#COUNTRY#") > 0){
			value = value.replaceAll("#COUNTRY#",countryObje);
		}
		if(value.contains("#LE_BOOK#")){
			value = value.replaceAll("#LE_BOOK#", leBookObje);
		}	
		if(value.contains("#BUSINESS_DATE#")){
			value = value.replaceAll("#BUSINESS_DATE#", businessDateObje);
		}	
		if(value.contains("#BUSINESS_LINE_ID#")){
			value = value.replaceAll("#BUSINESS_LINE_ID#", businessLineIdObje);
		}
		return value;
	}
	
	//private List<TAXEngineMappingTablVb> getEXLFilterTable(String taxLineId){
		private ExceptionCode getEXLFilterTable(String taxLineId){
		ExceptionCode exceptionCode = new ExceptionCode();
		List<TAXEngineMappingTablVb> commonVbs = new ArrayList<TAXEngineMappingTablVb>();
		String strQuery = new String("Select * "
						+ " from "+tablePrefix+"RA_Mst_TAX_Table_Mappings x1"
						+ " Where x1.Sequence_Status = 0 "
						+ "And x1.Country =  '"+countryObje+"'"
						+ " And x1.Le_Book = '"+leBookObje+"'"
						+ " And x1.Tax_Line_Id = '"+taxLineId+"'"
						+ " Order by Filter_Sequence ");
		try{
			rs = stmt.executeQuery(strQuery);
			while(rs.next()){
				TAXEngineMappingTablVb vobj = new TAXEngineMappingTablVb();
				if(rs.getString("Filter_Sequence") != null)
					vobj.setFilterSequence(rs.getString("Filter_Sequence"));
				if(rs.getString("Filter_Table") != null)
					vobj.setFilterTable(rs.getString("Filter_Table"));
				if(rs.getString("Condition_Value1") != null)
					vobj.setConditionValue1(rs.getString("Condition_Value1"));
				if(rs.getString("Condition_Value2") != null)
					vobj.setConditionValue2(rs.getString("Condition_Value2"));
				if(rs.getString("Condition_Operation") != null)
					vobj.setConditionOperation(rs.getString("Condition_Operation"));
				if(rs.getString("Filter_Column") != null)
					vobj.setFilterColumn(rs.getString("Filter_Column"));
				commonVbs.add(vobj);
			}
			exceptionCode.setErrorCode(SUCCESS_OPERATION);
			exceptionCode.setResponse(commonVbs);
		}catch(Exception e){

			exceptionCode.setErrorCode(ERROR_OPERATION);
			exceptionCode.setErrorMsg("Error while getting Valid Exclusion Filter Sequences ! Query["+strQuery+"] error messge ["+e.getMessage()+"]");
		}
		return exceptionCode;
	}
	
	//private List<TAXEngineTaxIDColVb> getTAXIDTable(){
	private ExceptionCode getTAXIDTable(){
		ExceptionCode exceptionCode = new ExceptionCode();
		List<TAXEngineTaxIDColVb> commonVbs = new ArrayList<TAXEngineTaxIDColVb>();
		String strQuery = new String(
				"SELECT distinct T1.TAX_LINE_ID, case when "
				+ " (select count(1) from "+tablePrefix+"RA_Mst_Tax_Table_Mappings T2 "
				+ "  Where T1.Country = T2.Country "
				+ "  And T1.Le_Book = T2.Le_Book "
				+ "  And T1.Tax_line_id = T2.Tax_Line_id And T2.Sequence_Status = 0) > 0 then 'Y' else 'N' end EXL_FILTER_FLAG  "
				+ " FROM "+tablePrefix+"RA_MST_TE_"+countryObje+leBookObje+businessDateObje_YYYYMMDD+"_"+businessLineIdObje+" T1"
				+ " ");
		try{
			rs = stmt.executeQuery(strQuery);
			while(rs.next()){
				TAXEngineTaxIDColVb vobj = new TAXEngineTaxIDColVb();
				if(rs.getString("TAX_LINE_ID") != null)
					vobj.setTaxLineId(rs.getString("TAX_LINE_ID"));
				if(rs.getString("EXL_FILTER_FLAG") != null)
					vobj.setFilterTableListFlag(rs.getString("EXL_FILTER_FLAG"));
				commonVbs.add(vobj);
			}
			exceptionCode.setErrorCode(SUCCESS_OPERATION);
			exceptionCode.setResponse(commonVbs);
		}catch(Exception e){
			exceptionCode.setErrorCode(ERROR_OPERATION);
			exceptionCode.setErrorMsg("Error while getting Valid Tax Idss ! Query["+strQuery+"] error messge ["+e.getMessage()+"]");
		}
		return exceptionCode;
	}
	
	private ExceptionCode getSourceMappingTable(){
		ExceptionCode exceptionCode = new ExceptionCode();
		List<TAXEngineSourceTabColVb> commonVbs = new ArrayList<TAXEngineSourceTabColVb>();
		String strQuery = new String("Select "+
				"TABLE_NAME, SOURCE_TABLE, TABLE_ALIAS, GENERIC_CONDITION, "+ 
        "case when JOIN_TYPE = 'INNER' then 'INNER JOIN' "+
             "when JOIN_TYPE = 'LEFT' then 'LEFT OUTER JOIN' "+
             "when JOIN_TYPE = 'RIGHT' then 'RIGHT OUTER JOIN' "+
        "else 'INNER JOIN' end JOIN_TYPE ,  "+
        	"'N' JOIN_STATUS "+
        "from "+tablePrefix+"RA_Mst_TAX_Source_Table  "+
		"Where Table_Status = 0");
		try{
			rs = stmt.executeQuery(strQuery);
			while(rs.next()){
				TAXEngineSourceTabColVb vobj = new TAXEngineSourceTabColVb();
				if(rs.getString("TABLE_NAME") != null)
					vobj.setTableName(rs.getString("TABLE_NAME"));
				if(rs.getString("SOURCE_TABLE") != null)
					vobj.setSourceTable(rs.getString("SOURCE_TABLE"));
				if(rs.getString("TABLE_ALIAS") != null)
					vobj.setTableAlias(rs.getString("TABLE_ALIAS"));
				if(rs.getString("GENERIC_CONDITION") != null)
					vobj.setGenericCondition(rs.getString("GENERIC_CONDITION"));
				if(rs.getString("JOIN_TYPE") != null)
					vobj.setJoinType(rs.getString("JOIN_TYPE"));
				if(rs.getString("JOIN_STATUS") != null)
					vobj.setJoinStatus(rs.getString("JOIN_STATUS"));
				commonVbs.add(vobj);
			}
			exceptionCode.setErrorCode(SUCCESS_OPERATION);
			exceptionCode.setResponse(commonVbs);
		}catch(Exception e){
			exceptionCode.setErrorCode(ERROR_OPERATION);
			exceptionCode.setErrorMsg("Error while getting Source Mapping Table ! Query["+strQuery+" error messge ["+e.getMessage()+"]");
		}
		return exceptionCode;
	}
	
	
	private ExceptionCode getCheckActualRecordCount() {
		
		ExceptionCode exceptionCode = new ExceptionCode();
		
		int recordCount = 0;
		
		StringBuilder temp_sb = new StringBuilder(); 
		
		
		if("ORACLE".equalsIgnoreCase(dataBaseType)) {
			
			temp_sb.append("SELECT count(1) COUNT From ");
			
		}else if("MSSQL".equalsIgnoreCase(dataBaseType)) {
			
			temp_sb.append("SELECT top(1) 1 COUNT From ");
			
		}else {
			temp_sb.append(" ***Invalid Database Type*** ");
		}
		
		if(P_Trans_Line_Type.equalsIgnoreCase("S")) {
			temp_sb.append(" "+tablePrefix+"RA_SERVICE_IE_TRANSACTIONS T1 ");
		}
		else {
			temp_sb.append(" "+tablePrefix+"RA_PRODUCT_IE_TRANSACTIONS T1 ");
		}
		
		temp_sb.append("WHERE T1.COUNTRY = '"+countryObje+"' ");
		temp_sb.append("AND T1.LE_BOOK = '"+leBookObje+"' ");
		temp_sb.append("AND T1.BUSINESS_LINE_ID = '"+businessLineIdObje+"' ");
		
		if("ORACLE".equalsIgnoreCase(dataBaseType)) {
			
			temp_sb.append("AND T1.BUSINESS_DATE = TO_DATE('"+businessDateObje+"','DD-MON-YYYY') ");
			temp_sb.append("AND rownum < 2 ");
			
		}else if("MSSQL".equalsIgnoreCase(dataBaseType)) {
			
			temp_sb.append("AND BUSINESS_DATE = convert(date,'"+businessDateObje+"') ");
			
		}else {
			temp_sb.append(" ***Invalid Database Type*** ");
		}
		
		try {
			rs = stmt.executeQuery(temp_sb.toString());
			if(rs.next()){
				recordCount = Integer.parseInt(rs.getString("COUNT"));
				exceptionCode.setErrorCode(SUCCESS_OPERATION);
				exceptionCode.setResponse(recordCount);
			}
			else {
				/*exceptionCode.setErrorCode(ERROR_OPERATION);
				exceptionCode.setErrorMsg("No data found while checking count in Actual table!!!");
				As we are using top 1 in mssql, it doesn;t always give a row like count(1) of Oracle*/
				exceptionCode.setResponse(0);
				exceptionCode.setErrorCode(SUCCESS_OPERATION);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(ERROR_OPERATION);
			exceptionCode.setErrorMsg("Error while checking Count from Actual IE Tables ! Query["+temp_sb.toString()+"] error messge ["+e.getMessage()+"]");
		}


		return exceptionCode;
	}
	
	private ExceptionCode getCheckTaxRecordCount() {
		
		ExceptionCode exceptionCode = new ExceptionCode();
		
		int recordCount = 0;
		String query = null;
		
		if("ORACLE".equalsIgnoreCase(dataBaseType)) {
			
			   query = "SELECT COUNT(1) COUNT FROM "+tablePrefix+"RA_MST_TE_"+countryObje+leBookObje+businessDateObje_YYYYMMDD+"_"+businessLineIdObje+""
					+ " Where Rownum < 2 ";
			
		}else if("MSSQL".equalsIgnoreCase(dataBaseType)) {
			
			   query = "SELECT top(1) 1 COUNT FROM "+tablePrefix+"RA_MST_TE_"+countryObje+leBookObje+businessDateObje_YYYYMMDD+"_"+businessLineIdObje+"";
			
		}else {
			   logWriter("*** Invalid Database Type ***");
		}

		try {
			rs = stmt.executeQuery(query);
			if(rs.next()){
				recordCount = Integer.parseInt(rs.getString("COUNT"));
				exceptionCode.setErrorCode(SUCCESS_OPERATION);
				exceptionCode.setResponse(recordCount);
			}
			else {/*
				exceptionCode.setErrorCode(ERROR_OPERATION);
				exceptionCode.setErrorMsg("No data found while checking count in RA_MST_TE temp table!!!");
				As we are using top 1 in mssql, it doesn;t always give a row like count(1) of Oracle*/
				exceptionCode.setResponse(0);
				exceptionCode.setErrorCode(SUCCESS_OPERATION);
			}
		} catch (SQLException e) {
//			e.printStackTrace();
			exceptionCode.setErrorCode(ERROR_OPERATION);
			exceptionCode.setErrorMsg("Error while checking Count from Tax Maintenence Table ! Query["+query+" error messge ["+e.getMessage()+"]");
		}
		
		return exceptionCode;
	}
	

	private ExceptionCode getTransLineType(){
		
		ExceptionCode exceptionCode = new ExceptionCode();
		
		String TRANS_LINE_TYPE ="";
		
		String query = "SELECT TRANS_LINE_TYPE  FROM "+tablePrefix+"RA_MST_BUSINESS_LINE_HEADER T1 "
				+ " WHERE T1.COUNTRY = '"+countryObje+"'"
				+ "AND T1.LE_BOOK = '"+leBookObje+"'"
				+ "AND T1.BUSINESS_LINE_ID = '"+businessLineIdObje+"'";
		
		try {
			rs = stmt.executeQuery(query);
			if(rs.next()){
				TRANS_LINE_TYPE = rs.getString("TRANS_LINE_TYPE");
				exceptionCode.setErrorCode(SUCCESS_OPERATION);
				exceptionCode.setResponse(TRANS_LINE_TYPE);
			}
			else {
				exceptionCode.setErrorCode(ERROR_OPERATION);
				exceptionCode.setErrorMsg("No data found in RA_MST_BUSINESS_LINE_HEADER for given arguments!!!");
			}
		} catch (SQLException e) {
//			e.printStackTrace();
			exceptionCode.setErrorCode(ERROR_OPERATION);
			exceptionCode.setErrorMsg("Error while checking Trans Line Type ! Query["+query+" error messge ["+e.getMessage()+"]");
		}

		return exceptionCode;
	}
	
	private ExceptionCode getVisionVarValue(){
		ExceptionCode exceptionCode = new ExceptionCode();
		String Vision_Var_Check ="";
		
		String query = "SELECT T1.Value FROM "+tablePrefix+"Vision_Variables T1  "
				+ " WHERE T1.Variable = 'RA_TAX_MODULE_COMPUTE_FLAG' ";
		
		try {
			rs = stmt.executeQuery(query);
			if(rs.next()){
				Vision_Var_Check = rs.getString("VALUE");
				exceptionCode.setErrorCode(SUCCESS_OPERATION);
				exceptionCode.setResponse(Vision_Var_Check);
			}
			else {
				exceptionCode.setErrorCode(ERROR_OPERATION);
				exceptionCode.setErrorMsg("No data found in Vision Variables for RA_TAX_MODULE_COMPUTE_FLAG flag!!!");
			}
		} catch (SQLException e) {
//			e.printStackTrace();
			exceptionCode.setErrorCode(ERROR_OPERATION);
			exceptionCode.setErrorMsg("Error while checking VisionVariable ! Query["+query+" error messge ["+e.getMessage()+"]");
		}
		return exceptionCode;
	}
	
	
	private void doFinishingSteps(){
		try{
			if(connectionMain != null && !connectionMain.isClosed())
				connectionMain.close();
		}
		catch(Exception e){
			//:TODO : Log the user friendly exception message 
		}
	}
	
	
	public int connnectToDatabase(String jdbcUrl, String username, String password, String databaseType) throws ClassNotFoundException{
		try{
			logWriter("VISION_APP_CONNECT_STRING : "+jdbcUrl);
			logWriter("VISION_USER_NAME : "+username);
			logWriter("VISION_PASSWORD : *********");
			logWriter("RA_DATABASE_TYPE : "+databaseType);
			logWriter("VISION_PASSWORD_ENCRYPT_FLAG : "+passwordEncryptFlag);
			String secretKey = "";
			if(isValid(passwordEncryptFlag) && passwordEncryptFlag.equals("Y")) {
				secretKey = System.getenv("VISION_DB_PASSWORD_ENCRYPT_KEY_N4");
				if(isValid(secretKey)) {
					logWriter("VISION_DB_PASSWORD_ENCRYPT_KEY_N4 : ************");
				}else {
					secretKey = System.getenv("VISION_DB_PASSWORD_ENCRYPT_KEY");
					logWriter("VISION_DB_PASSWORD_ENCRYPT_KEY : ************");
				}
				if(!isValid(secretKey)) {
					secretKey = "vision123";
					logWriter("Password decryption using default secret Key[*********]");
					logWriter("Maintain Jasypt encryption secret Key in variable [VISION_DB_PASSWORD_ENCRYPT_KEY] ");
				}
				try {
					password = EncryptDecryptFunctions.jaspytPasswordDecrypt(password,secretKey);	
				}catch(Exception e){
					logWriter(e.getMessage());
					logWriter("Error while decrypting Jasypt password.Exit further processing...!! ");
					return ERROR_OPERATION;
				}
				
			}
			
			if("ORACLE".equalsIgnoreCase(databaseType)) {
				Class.forName("oracle.jdbc.driver.OracleDriver");
			//	connectionMain = DriverManager.getConnection(jdbcUrl,"vision","visionprod");
				connectionMain = DriverManager.getConnection(jdbcUrl,username,password);
				connectionMain.setAutoCommit(true);
			}else if("MSSQL".equalsIgnoreCase(databaseType)) {
				Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
				/*String connectionUrl =
		                "jdbc:sqlserver://yourserver.database.windows.net:1433;"
		                        + "database=AdventureWorks;"
		                        + "user=yourusername@yourserver;"
		                        + "password=yourpassword;"
		                        + "encrypt=true;"
		                        + "trustServerCertificate=false;"
		                        + "loginTimeout=30;";*/
				
				String connectionUrl = jdbcUrl+";user="+username+";password="+password;
				
				connectionMain = DriverManager.getConnection(connectionUrl);
			}else {
				logWriter("Error connecting to the database !!!!!!- Invalid database type ["+databaseType+"]");
				return ERROR_OPERATION;
			}
			
			return SUCCESS_OPERATION;
		}catch(SQLException e){
			e.printStackTrace();
			logWriter("Error connecting to the database !!!!!!-" +e.getMessage());
			return ERROR_OPERATION;
		}
	}
	
	private int executeStatment(String query, boolean printCountFlag, String logType){
		int retVal =  0;
		try {
			retVal = stmt.executeUpdate(query);
			
			if(printCountFlag)
			{
				logWriter("["+retVal+"] records processed for ("+logType+") operation.");
			}
		} catch (SQLException e) {
//			e.printStackTrace();
			logWriter(e.getMessage());
			logWriter("Errored Query:["+query+"]");
			return ERROR_OPERATION;
		}
		return SUCCESS_OPERATION;
	}
	
	
	private boolean isValid(String pInput) {
		return !((pInput == null) || (pInput.trim().length() == 0) || ("".equals(pInput)));
	}
	
	/*private void logWriter(String logText) {
		LocalDateTime now = LocalDateTime.now();  
	    localDateTime = dtf_local.format(now);
		System.out.println("["+localDateTime+"]:"+logText);
	}*/
	
	private String getCurrentDateOnly() {
		String currentDate = "";
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal = Calendar.getInstance();
		currentDate = dateFormat.format(cal.getTime());
		return currentDate;
	}
	public String getCurrentDateTime() {
		DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		Date date = new Date();
		return dateFormat.format(date);
	}
	
	private void logWriter(String logText) {
		try {
			LocalDateTime now = LocalDateTime.now();  
		    localDateTime = dtf_local.format(now);
		    //logFileNameObje = countryObje+leBookObje+"_"+businessLineIdObje+"_"+businessDateObje_YYYYMMDD+"_"+getCurrentDateOnly()+".log";
			logfile = new FileWriter(logPathObje, true);
			bufferedWriter = new BufferedWriter(logfile);
			bufferedWriter.newLine();
			bufferedWriter.write(dynamicProgramName+":"+businessLineIdObje+":"+localDateTime+": " + logText);
			bufferedWriter.close();
			logfile.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}

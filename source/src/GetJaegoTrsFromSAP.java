package com.isp.sap.process;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Vector;

import oracle.jdbc.OracleTypes;

import com.isp.common.constants.IspConstantsIF;
import com.isp.common.util.JCO30Client;
import com.isp.common.util.OraclePackageUtil;
import com.posdata.glue.PosException;
import com.posdata.glue.biz.activity.PosActivity;
import com.posdata.glue.biz.activity.PosServiceParamIF;
import com.posdata.glue.biz.constants.PosBizControlConstants;
import com.posdata.glue.context.PosContext;
import com.posdata.glue.dao.PosGenericDao;
import com.sap.conn.jco.JCoTable;
import com.tobesoft.xplatform.data.DataSet;
import com.tobesoft.xplatform.data.DataTypes;

public class GetJaegoTrsFromSAP extends PosActivity
{
	/*** 
	 * SAP재고현황  SM00150
	 */
	@Override
	public String runActivity(PosContext ctx)
	{

		String strReturn		= PosBizControlConstants.SUCCESS;
		String isAudit 			= this.getProperty(PosServiceParamIF.IS_AUDIT);
		
//		Object objDs 			= ctx.get(getProperty(IspConstantsIF.INPUT_NAME)); 		//항곰 리스트 데이터 셋
		//DataSet dsData 		= (DataSet)objDs;
		//List listData 		 	= Converter.convertDataSetToList(dsData);
		
		String procedureId 	= this.getProperty(IspConstantsIF.PROCEDURE_ID) == null ? "": this.getProperty(IspConstantsIF.PROCEDURE_ID).trim().toUpperCase() ; 
		if("".equals(procedureId)) {
			throw new PosException("IspOraclePackageSearch Error : Procedure is Null");
		}
		
		String strPlant 		= (String)ctx.get(this.getProperty("param1"));
		String strErp 			= (String)ctx.get(this.getProperty("param2"));
		String strKind 			= (String)ctx.get(this.getProperty("param3"));
		
		if("undefined".equals(strErp)){
			strErp="";
		}
		if("undefined".equals(strKind)){
			strKind="";
		}

		PosGenericDao dao 	 	= this.getDao(this.getProperty(PosServiceParamIF.DAO));
		Connection con 			= dao.getDBConnection();
		
		String datasetNm 		= this.getProperty("resultkey");
		DataSet dsData 			= new DataSet(datasetNm);
		
		HashMap<String, String> request 		= new HashMap<>();
		Vector<HashMap<String, String>> requestAl 		= new Vector<HashMap<String, String>>();
		int Row 				= 0; 
		
		//**************Audit***************************//*
		if(Boolean.valueOf(isAudit).booleanValue()){
	        try(CallableStatement cs = con.prepareCall(IspConstantsIF.AUDIT_SQL))	//  "{call SP_INIT_PROC(?,?,?) }";
	        {
	        	cs.setString(1,"IF-SAP");
	        	cs.setString(2,ctx.getAuditAttribute().getProgramId());
	        	cs.setString(3,ctx.getAuditAttribute().getObjectType());
	        	cs.execute();
	        }
	        catch (SQLException e) {
				logger.logError("SQLException error::"+e.getMessage());
				throw new PosException("Error : " +  e.getMessage());
			}
		}
		
		//***************Audit***********************//*
		String callQuery 	 = OraclePackageUtil.getProcedureInfo(ctx, procedureId, dao);
		try(CallableStatement cs = con.prepareCall(callQuery))
		{
			cs.setString(1,  strPlant);
			cs.setString(2,  strErp);
			cs.setString(3,  strKind);
			cs.registerOutParameter(4, OracleTypes.CURSOR);
			cs.executeQuery();
			
			if(cs.getObject(4) != null) {
				try(ResultSet rs = (ResultSet)cs.getObject(4))
				{
				
					//***************I/F 시작***********************//
					dsData.addColumn("MATNR", DataTypes.STRING, 200);
					dsData.addColumn("WERKS", DataTypes.STRING, 200);
					dsData.addColumn("LGORT", DataTypes.STRING, 200);
					dsData.addColumn("LABST", DataTypes.BIG_DECIMAL, 10);
					dsData.addColumn("MEINS", DataTypes.STRING, 200);
					dsData.addColumn("MATR_NAME", DataTypes.STRING, 200);
					
					String strRfcId="ZMM_SAP_JAEGO_TRS_NEW";  //수신 ID
					JCO30Client j4 = new JCO30Client(strRfcId); //창고별 재고 현황
					JCoTable tbMatDoc = null;
					while(rs.next())
					{
						j4 = new JCO30Client(strRfcId); //창고별 재고 현황
						request 		= new HashMap<String, String>();
						requestAl 	= new Vector<HashMap<String, String>>();
						request.put("MATNR", rs.getString("MATR_CD"));
						request.put("WERKS", rs.getString("PLANT"));
						request.put("LGORT", rs.getString("ERP_WHG_SAVE_LOC"));
						request.put("LABST", "");
						request.put("MEINS", "");
						requestAl.add(request);
						
//							    JCoTable tbRequest = j4.getTable("MATERIAL");		//?  tbRequest 왜 받는지?
						j4.getTable("MATERIAL");		//?  tbRequest 왜 받는지?
						
						j4.setMpramTable("MATERIAL", requestAl);  //자재 번호
						j4.execute();
						
						tbMatDoc = j4.getTable("JAEGO");
						
						if(tbMatDoc.getNumRows() > 0){ 
							for (int ix = 0; ix < tbMatDoc.getNumRows(); ix++)
							{	 
								Row = dsData.newRow(); 
								dsData.set(Row, "MATNR", tbMatDoc.getValue("MATNR"));	
								dsData.set(Row, "WERKS", tbMatDoc.getValue("WERKS"));
								dsData.set(Row, "LGORT", tbMatDoc.getValue("LGORT"));
								dsData.set(Row, "LABST", tbMatDoc.getValue("LABST"));
								dsData.set(Row, "MEINS", tbMatDoc.getValue("MEINS"));
								dsData.set(Row, "MATR_NAME", rs.getString("MATR_NAME"));
								
								tbMatDoc.nextRow();
							}
						}
					}
					//***************I/F 끝************************//	
				}
				catch(SQLException e){
					logger.logError("SQLException ResultSet error::"+e.getMessage());
					strReturn = PosBizControlConstants.FAILURE;
				}
			}
			ctx.put(datasetNm, dsData);
			strReturn = PosBizControlConstants.SUCCESS;
		}
		catch(SQLException e){
			logger.logError("SQLException error::"+e.getMessage());
			strReturn = PosBizControlConstants.FAILURE;
		}
		return strReturn;
    }
}

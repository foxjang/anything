package com.isp.sap.process;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.isp.common.constants.IspConstantsIF;
import com.isp.common.util.IspUtil;
import com.isp.common.util.OraclePackageUtil;
import com.posdata.glue.PosException;
import com.posdata.glue.biz.activity.PosActivity;
import com.posdata.glue.biz.activity.PosServiceParamIF;
import com.posdata.glue.biz.constants.PosBizControlConstants;
import com.posdata.glue.context.PosContext;
import com.posdata.glue.dao.PosGenericDao;
import com.posdata.glue.xplatform.common.Converter;
import com.tobesoft.xplatform.data.DataSet;

@SuppressWarnings("rawtypes")
public class GetUserrFromDBLINK extends PosActivity
{
	/*** 
	 * 사원정보  RM00110
	 */
	@Override
	public String runActivity(PosContext ctx)
	{
		String strReturn		= PosBizControlConstants.SUCCESS;
		String isAudit 			= this.getProperty(PosServiceParamIF.IS_AUDIT);

		//정주기 작업 프로시져
		String procedureWork 	= this.getProperty("procedure-work");   
		//데이터 업데이트
		String ProcedureSave 	= this.getProperty("procedure-save");
			
		String strPlant			= (String) ctx.get("PlantCd");	
		
		Object objDs 			= ctx.get(getProperty(IspConstantsIF.INPUT_NAME)); 		//항곰 리스트 데이터 셋
		DataSet dsData 		= (DataSet)objDs;
		List listData 		 	= Converter.convertDataSetToList(dsData);
		
		String strtDtm          = IspUtil.getCurrentDate("yyyyMMddHHmmss");
		String endDtm           = null;
		String suceType 		= null;
		
		PosGenericDao dao 	 	= this.getDao(this.getProperty(PosServiceParamIF.DAO));
		Connection con 			= dao.getDBConnection();
		
		if(listData!=null && listData.size()>0) {
			//**************Audit***************************//*
			if(Boolean.valueOf(isAudit).booleanValue()){
				try(CallableStatement cs = con.prepareCall(IspConstantsIF.AUDIT_SQL))		//  "{call SP_INIT_PROC(?,?,?) }";
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
			//***************Audit***********************//
			
			try
			{	
				//***************정주기 작업 프로시져 시작 업데이트***********************//*
				SapCommon.UpdateWorkPlan(ctx, procedureWork, dao, con, listData, strtDtm, endDtm, suceType);
				//***************정주기 작업 프로시져 시작 업데이트***********************//*
			}
			catch(SQLException e) {
				logger.logError("SQLException error::"+e.getMessage());
				throw new PosException("정주기 작업 프로시져 시작 업데이트 Error : " +  e.getMessage());
			}
			
			String callQuery 	      = OraclePackageUtil.getProcedureInfo(ctx, ProcedureSave, dao);
//					int colcont 		      =(int) OraclePackageUtil.getMember(ctx,IspConstantsIF.PARAM_COUNT);
//					String[] arParams 		  =(String[]) OraclePackageUtil.getMember(ctx, IspConstantsIF.PARAMS);  
//					String[] arParamsDataType =(String[]) OraclePackageUtil.getMember(ctx, IspConstantsIF.PARAMS_DATA_TYPE); 
			
			//***************시작***********************//
			try(CallableStatement cs = con.prepareCall(callQuery))
			{
				cs.setString(1, strPlant);
				cs.execute();
				
				endDtm   = IspUtil.getCurrentDate("yyyyMMddHHmmss");	
				suceType  = "성공";
			}
			catch(SQLException e)
			{
				endDtm   =IspUtil.getCurrentDate("yyyyMMddHHmmss");
				suceType = e.getMessage();
				logger.logError("SQLException error::"+e.getMessage());
				strReturn = PosBizControlConstants.FAILURE;
			}
			
			try {
				//***************정주기 작업 프로시져 시작 업데이트***********************//*
				SapCommon.UpdateWorkPlan(ctx, procedureWork, dao, con, listData, strtDtm, endDtm, suceType);
				//***************정주기 작업 프로시져 시작 업데이트***********************//*
			} catch (Exception e) {
				logger.logError("Exception error::"+e.getMessage());
				strReturn = PosBizControlConstants.FAILURE;
			}
		}
		
		return strReturn;
	}
}

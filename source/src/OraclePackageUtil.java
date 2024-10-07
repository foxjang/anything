/*========================================================
*Copyright(c) 2011 POSCO CHEMTECH/POSCO ICT
*@ProcessChain   : common
*@FileName       : IspUtil.java
*@FileTitle      : Common Util
*Change history
*@LastModifier   : 윤근수
*@LastVersion    :  1.0
*    2012-02-09   차상환
*        1.0      최초 생성
*    2012-05-24   윤근수
*        1.1      추가
=========================================================*/
package com.isp.common.util;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;

import com.isp.common.constants.IspConstantsIF;
import com.posdata.glue.context.PosContext;
import com.posdata.glue.dao.PosGenericDao;
import com.posdata.glue.dao.vo.PosColumnDef;
import com.posdata.glue.dao.vo.PosParameter;
import com.posdata.glue.dao.vo.PosRowSet;
import com.posdata.glue.util.log.PosLog;
import com.posdata.glue.util.log.PosLogFactory;
import com.tobesoft.xplatform.data.DataSet;
import com.tobesoft.xplatform.data.DataTypes;
@SuppressWarnings("rawtypes")
public class OraclePackageUtil 
{
	private static PosLog logger = PosLogFactory.getLogger(OraclePackageUtil.class);

	private static final int MAX_BUF_BYTE = 1024000;

	/**
	 * 패키지 메타 데이터를 가져와 SQL 문을 동적으로 생성시킨다
	 * @param ctx
	 * @param PackageName
	 * @param ProcedureName
	 * @return
	 */
	public static String getProcedureInfo(PosContext ctx, String sProcedureName, PosGenericDao dao) {
		//PosGenericDao dao 		= this.getDao(this.getProperty(PosServiceParamIF.DAO));
		StringBuffer procState 	= new StringBuffer();
		PosParameter param;
		HashMap<String, Object> localHashMap 	= new HashMap<String, Object>();
		procState.append("{call ").append(sProcedureName);
		
		try{
			//쿼리실행
			param 			= new PosParameter(); 
			String sSql 	="oracle_arguments.procedure"; //쿼리 ID
			String[] prm	= sProcedureName.toString().split("\\.");
			param.setWhereClauseParameter(0, IspUtil.strNullChk(prm[0]));
			param.setWhereClauseParameter(1, IspUtil.strNullChk(prm[1]));
			PosRowSet rs	=	dao.find(sSql, param);

			int iParamCount   = rs.count();
			String[] arParams = null;
			String[] dataTypeParams = null;
			String sInOut     = "";
			String sParam     = "";
			if (iParamCount > 0){
				arParams 		= new String[iParamCount];
				dataTypeParams 	= new String[iParamCount];
				int prmCnt =0;
                for(int i=0; i < iParamCount;i++) {
                    sParam += ", :" +(String)rs.getAllRow()[i].getAttribute("ARGUMENT_NAME");
                    sInOut = (String)rs.getAllRow()[i].getAttribute("IN_OUT");
                    
					if (sInOut.equals("IN")){
						arParams[prmCnt] 		= (String)rs.getAllRow()[i].getAttribute("ARGUMENT_NAME");
						dataTypeParams[prmCnt]  = (String)rs.getAllRow()[i].getAttribute("DATA_TYPE");
						prmCnt++;
					}
					else{
						//arParams[i] = "";
					}
				
                }
				procState.append("(").append(sParam.substring(1)).append(") }");
			}// end of if (iParamCount > 0)
			else{
				procState.append(" }");
			}
			localHashMap.put( IspConstantsIF.PARAMS, arParams);
			localHashMap.put( IspConstantsIF.PARAMS_DATA_TYPE, dataTypeParams);
			localHashMap.put( IspConstantsIF.PARAM_COUNT, iParamCount);
			ctx.put("_member", localHashMap);

		}
		catch(Exception e){
			 logger.logError("getProcedureInfo Exception : " + e.getMessage());
		}

		return procState.toString();
	}// end of getProcedureInfo
	
	public static Object getMember(PosContext paramGlueContext, String paramString)
	{
	    HashMap localHashMap = (HashMap)paramGlueContext.get("_member");
	    Object localObject = localHashMap.get(paramString);
	    return localObject;
	}
	
	public static  String padRight(String s, BigDecimal bigDecimal) {
	     return String.format("%-" + bigDecimal + "s", s);  
	}

	public static  String padLeft(String s, int n) {
	    return String.format("%" + n + "s", s);  
	}
   
	public static PosColumnDef[] a(ResultSet paramResultSet)throws SQLException {
		ResultSetMetaData localResultSetMetaData = paramResultSet.getMetaData();
		int i = localResultSetMetaData.getColumnCount();
		PosColumnDef[] arrayOfPosColumnDef = new PosColumnDef[i];
		int j = 0;
		for (int k = 1; j < i; k++)
		{
			String str = localResultSetMetaData.getColumnName(k);
			int m = localResultSetMetaData.getColumnType(k);
			int n = -1;
			int i1 = -1;
			int i2 = -1;
			if ((2004 != m) && (2005 != m)) {
				try
				{
					n = localResultSetMetaData.getColumnDisplaySize(k);
					i1 = localResultSetMetaData.getPrecision(k);
					i2 = localResultSetMetaData.getScale(k);
				}
				catch (Exception localException) {
					logger.logError("PosColumnDef Exception : " + localException.getMessage());				
				}
			}
			arrayOfPosColumnDef[j] = new PosColumnDef(str, m, n, i1, i2);j++;
		}
		return arrayOfPosColumnDef;
  }
	 
	
	/***
	 *  String 변환 
	 * @param classType
	 * @param object
	 * @return String 
	 */

	public static String getDataChString(Class<? extends Object> classType, Object object)
    {
		String returnValu ="";
		if(classType == BigDecimal.class ){
			returnValu = String.valueOf((BigDecimal)object);
		}else if(classType ==int.class){
			returnValu = String.valueOf((int)object);
		}else if(classType == float.class){
			returnValu = String.valueOf((float)object);
		}else if(classType == double.class){
			returnValu = String.valueOf((double)object);
		}else if(classType == Integer.class){
			returnValu = String.valueOf((Integer)object);
		}else{
			returnValu = (String)object;
		}
        return returnValu;
    }
	
	/***
	 *  BigDecimal 변환 
	 * @param classType
	 * @param object
	 * @return String 
	 */
	public static BigDecimal getDataChBigDecimal(Class<? extends Object> classType, Object object)
    {
		BigDecimal obj =null;
		if(classType == BigDecimal.class ){
			obj = (BigDecimal)object;
		}else if(classType ==int.class){
			obj = new BigDecimal((int)object);
		}else if(classType == float.class){
			obj = new BigDecimal((float)object);
		}else if(classType == double.class){
			obj = new BigDecimal((double)object);
		}else if(classType == Integer.class){
			obj = new BigDecimal((Integer)object);
		}else{
			obj = new BigDecimal((String)object);
		}
        return obj;
    }


	/***
	 *  Adit 항목 Setting 
	 * @param PosContext
	 * @param PosGenericDao
	 * @return Boolean
	 * 
	 * PKG_VARIABLE : Oracle 전역 변수로 Aduit 항목 Set
	 */
	public static Boolean setAditInitProc(PosContext ctx, PosGenericDao dao, Connection con) throws SQLException
	{
		Boolean rtnB = false;
        try(CallableStatement cs = con.prepareCall(IspConstantsIF.AUDIT_SQL))
        {
        	cs.setString(1,(String) ctx.get(IspConstantsIF.G_USER_ID));
        	cs.setString(2,ctx.getAuditAttribute().getProgramId());
        	cs.setString(3,ctx.getAuditAttribute().getObjectType());
        	cs.execute();
        	
        	rtnB = true;
        }
		return rtnB;
	}


	/*
	 * ResultSet- > DataSet Head  생성
	 */
	public static DataSet makeDataSet(ResultSet rs,String strDataSet) throws SQLException
	{
		DataSet ds = new DataSet(strDataSet);
//		ds.setU setUpdate(false);

		ResultSetMetaData rsmd = rs.getMetaData();     // select 한 정보
		int numberOfColumns = rsmd.getColumnCount();   // select한 컬럼수

		int    ColSize;
		int    ColType;
		String Colnm;
		for ( int j = 1 ; j <= numberOfColumns ; j++ )
		{
			Colnm = rsmd.getColumnName(j);
			ColType = rsmd.getColumnType(j);
			ColSize = rsmd.getColumnDisplaySize(j);

			// select한 컬럼의 type에 맞게 데이타셋 컬럼을 생성
			if ( ColType == Types.NUMERIC || ColType == Types.DOUBLE || ColType == Types.INTEGER )
			{
				ds.addColumn(Colnm, DataTypes.BIG_DECIMAL, ColSize);
			}
			else if ( ColType == Types.VARCHAR )
			{
				ds.addColumn(Colnm, DataTypes.STRING, ColSize);
			}
			else if ( ColType == Types.DATE )
			{
				ds.addColumn(Colnm, DataTypes.DATE, ColSize);
			}
			else if ( ColType == Types.BLOB )
			{
				ds.addColumn(Colnm, DataTypes.BLOB ,ColSize);
			}
			else if ( ColType == Types.CLOB )
			{
				ds.addColumn(Colnm, DataTypes.STRING ,ColSize);
			}
			else
			{
				ds.addColumn(Colnm, DataTypes.STRING ,ColSize);
			}
		}
		return ds;
	}


	/*
	 * ResultSet- > DataSet Row Data 변환
	 */
	public static DataSet setData_DataSet(ResultSet rs, DataSet ds) throws SQLException, Exception
	{
	
		ResultSetMetaData rsmd = rs.getMetaData();     // select 한 정보
		int numberOfColumns = rsmd.getColumnCount();   // select한 컬럼수
		int Row = 0;
		Object obj = null; 
		String strObj = null; 
		String colNm = "";
		int dataType;
		
		while(rs.next()){
			Row = ds.newRow();   // 데이타셋 row 추가
			for (int i = 0 ; i < numberOfColumns ; i++ )
			{
				colNm = ds.getColumn(i).getName();
				dataType = ds.getColumnDataType(colNm);
				
				if(dataType == DataTypes.BLOB) {
					if(rs.getBlob(colNm) != null ) {
//						ds.set(Row, colNm, setBlob(((OracleResultSet)rs).getBLOB(colNm)));
							ds.set(Row, colNm, setBlob(rs.getBlob(colNm)));
					}
					else {
						obj = null;
						ds.set(Row, colNm, obj);
					}
				}
				else if(dataType == DataTypes.STRING) {
					if(rs.getString(colNm) != null ) {
						strObj = rs.getString(colNm);
					}
					else {
						strObj = null;
					}
					ds.set(Row, colNm, strObj);  // 데이타저장
				}
				else {
					if(rs.getObject(colNm) != null ) {
						obj = rs.getObject(colNm);
					}
					else {
						obj = null;
					}
					ds.set(Row, colNm, obj);  // 데이타저장
				}	
			}	
		
		}
		return ds;
	}


	/*
	 * ResultSet -> BLOB 변환
	 */
	public static byte[] setBlob(Blob blob) throws SQLException, Exception
	{
		InputStream instream = blob.getBinaryStream();
	    ByteArrayOutputStream baos = new ByteArrayOutputStream();

	    byte v_data[] = new byte[MAX_BUF_BYTE];
	    int v_len = -1;
	    
	    while((v_len = instream.read(v_data)) != -1 )
	    {
	      baos.write(v_data, 0, v_len); 
	    }
	    
		return baos.toByteArray();
		
	}	
 }

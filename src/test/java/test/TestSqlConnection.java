package test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.schema.SchemaRequest.Fields;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.junit.Test;

import com.db.connect.util.DatabaseConnectUtil;
import com.db.connect.util.DatabasetableField;
import com.db.connect.util.SolrConnectUtil;
import com.solr.index.util.IndexHandleUtil;

/**
 * @author pms
 * @time 2017年11月1日上午11:58:58
 */
public class TestSqlConnection {
	@Test
	public void testConnection() throws SolrServerException, IOException {
		DatabaseConnectUtil db = new DatabaseConnectUtil();
		Connection cn = db.getJshConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
		String tableName = "ACTIVITY_TYPE";
		StringBuilder url = new StringBuilder("http://localhost:8983/solr");
		String sql = "select * from " + tableName;
		SolrClient solr = SolrConnectUtil.getSolrConnect(url.toString());
		SolrClient server = null;
		try {
			ps = cn.prepareStatement(sql);
			rs = ps.executeQuery();
			// 数据库列名
			String coreName = tableName + "_CORE";
			SolrConnectUtil.createNewCore(solr, coreName, "account_core");
			solr.close();
			server = SolrConnectUtil.getSolrConnect(url.append("/").append(coreName).toString());
			IndexHandleUtil.addResultSetIndex(rs, server, "ID");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DatabaseConnectUtil.closeConnection(cn, ps, rs);
		}
	}

	

	

}

package com.solr.index.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import com.db.connect.util.DatabasetableField;

/**
 * @author pms
 * @time 2017年11月28日下午5:26:21
 */
public class IndexHandleUtil {

	/**
	 * 根据数据中查询的结果集创建索引
	 * 
	 * @param rs
	 *            数据库查询结果
	 * @param server
	 *            solr服务
	 * @param keyField
	 *            排重字段
	 */
	public static void addResultSetIndex(ResultSet rs, SolrClient server, String keyField) {
		SolrDocumentList results = null;
		ResultSetMetaData rsmd;
		try {
			rsmd = rs.getMetaData();

			int cols = rsmd.getColumnCount();
			while (rs.next()) {
				Collection<SolrInputDocument> docs = new ArrayList<>();
				String s = "";
				SolrInputDocument doc = new SolrInputDocument();
				for (int i = 1; i <= cols; i++) {
					String k = rsmd.getColumnName(i);
					Object v = DatabasetableField.getResultFieldValue(rs, rsmd, i);
					if (v == null) {
						v = "";
					}
					if (keyField.equals(k) && v != null) {
						SolrQuery query = new SolrQuery(keyField + ":" + v);
						query.addField(keyField);
						QueryResponse resp = server.query(query);
						results = resp.getResults();
						if (results.getNumFound() > 0) {
							break;
						}
					}
					doc.addField(k, v);
					s += k + ":" + v + ",";
				}
				if (results.getNumFound() > 0) {
					continue;
				}
				docs.add(doc);
				server.add(docs);
				System.out.println(s + "\n");
				Thread.sleep(100);
				server.commit(true, true, true);

			}
			server.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 删除索引
	 * 
	 * @param solr
	 * @param key
	 *            查询字段
	 * @param value
	 *            字段值
	 */
	public static void deleteIndex(SolrClient solr, String key, String value) {
		try {
			solr.deleteByQuery(key + ":" + value);
			solr.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 根据javabean添加或者更新索引
	 * 
	 * @param solr
	 * @param T
	 */
	public void addOrUpdateIndex(SolrClient solr, Class<?> T) {
		try {
			solr.addBean(T);
			solr.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param <T>
	 * @param solr
	 * @param condition  查询条件
	 * @param start
	 * @param end
	 * @return
	 */
	public <T> List<T> queryIndex(SolrClient solr, Class<T> T, Map<String, Object> condition, int start, int end) {
		List<T> result = new ArrayList<>();
		try {
			Iterator<Entry<String, Object>> keySet = condition.entrySet().iterator();
			StringBuilder query = new StringBuilder();
			while (keySet.hasNext()) {
				Entry<String, Object> set = keySet.next();
				query.append(set.getKey()).append(":").append(set.getValue()).append("+");
			}
			// 查询条件
			SolrQuery solrParams = new SolrQuery();
			solrParams.setStart(start);
			solrParams.setRows(end);
			solrParams.setQuery(query.toString());
			// 开启高亮
			solrParams.setHighlight(true);
			solrParams.setHighlightSimplePre("<font color='red'>");
			solrParams.setHighlightSimplePost("</font>");

			// 设置高亮的字段
			solrParams.setParam("hl.fl", "name,description");
			// SolrParams是SolrQuery的子类
			QueryResponse queryResponse = solr.query(solrParams);

			// (一)获取查询的结果集合
			SolrDocumentList solrDocumentList = queryResponse.getResults();

			// (二)获取高亮的结果集
			// 第一个Map的键是文档的ID，第二个Map的键是高亮显示的字段名
			Map<String, Map<String, List<String>>> highlighting = queryResponse.getHighlighting();

			for (SolrDocument solrDocument : solrDocumentList) {
				Map<String, List<String>> fieldsMap = highlighting.get(solrDocument.get("id"));
				List<String> highname = fieldsMap.get("name");
				List<String> highdesc = fieldsMap.get("description");
			}

			// (三) 将响应结果封装到Bean
			result = (List<T>) queryResponse.getBeans(T);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;

	}
}

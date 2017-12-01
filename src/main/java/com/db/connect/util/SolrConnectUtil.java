package com.db.connect.util;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.common.util.NamedList;

/**
 * @author pms
 * @time 2017年11月2日下午5:32:45
 */
public class SolrConnectUtil {
	static SolrClient solrConnect;
	
	private SolrConnectUtil() {
		this.solrConnect = null;
	}

	/**
	 * 连接core
	 * @param url
	 * @return
	 */
	public static SolrClient getSolrConnect(String url) {
		if (solrConnect == null) {
			solrConnect = new HttpSolrClient.Builder(url).build();
		}
		return solrConnect;

	}

	/**
	 * 关闭solr连接
	 * @param solr
	 */
	public static void closeSolrConnect(SolrClient solr) {
		if (solr != null) {
			try {
				solr.commit();
				solr.close();
			} catch (SolrServerException | IOException e) {
				e.printStackTrace();
			}

		}

	}

	/**
	 * 根据已存在的core创建新的core
	 * @param server 
	 * @param coreName 新core名
	 * @param defaultCore 已经存在的core
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public static void createNewCore(SolrClient server,String coreName,String defaultCore) throws SolrServerException, IOException {
		// 获得solr.xml配置好的cores作为默认，获得默认core的路径
		NamedList<Object> newCoreList = CoreAdminRequest.getStatus(coreName, server).getCoreStatus().get(coreName);
		if (newCoreList.size() > 0) {
			return;
		}
		NamedList<Object> list = CoreAdminRequest.getStatus(defaultCore, server).getCoreStatus().get(defaultCore);
		String path = (String) list.get("instanceDir");

		// 获得solrhome,也就是solr放置索引的主目录
		String solrHome = path.substring(0, path.indexOf(defaultCore));

		// 建立新core所在文件夹
		File corePath = new File(solrHome + File.separator + coreName);
		if (!corePath.exists()) {
			corePath.mkdir();
		}
		// 建立新core下的conf文件夹
		File confPath = new File(corePath.getAbsolutePath() + File.separator + "conf/");
		if (!confPath.exists()) {
			confPath.mkdir();
		}
		// 将默认core下conf里的solrconfig.xml和schema.xml拷贝到新core的conf下。这步是必须的
		// 因为新建的core solr会去其conf文件夹下找这两个文件，如果没有就会报错，新core则不会创建成功
		/*FileUtils.copyFile(new File(path + "/conf/solrconfig.xml"),
				new File(confPath.getAbsolutePath() + File.separator + "solrconfig.xml"));
		
		FileUtils.copyFile(new File(path + "/conf/schema.xml"),
				new File(confPath.getAbsolutePath() + File.separator + "schema.xml"));*/
		File dir = new File(solrHome+"configsets/_default/conf");
		File[] dirs = dir.listFiles();
		for(File d : dirs) {
			if(d.isDirectory()) {
				File[] chirlds = d.listFiles();
				for(File c : chirlds) {
					File chirldrenPath = new File(confPath.getAbsolutePath() + File.separator+"lang");
					if(!chirldrenPath.exists()) {
						chirldrenPath.mkdirs();
					}
					FileUtils.copyFile(c,new File(chirldrenPath+ File.separator+c.getName()));
				}
			}else {
				FileUtils.copyFile(d,new File(confPath+ File.separator+d.getName()));
			}
		}

		// 创建新core,同时会把新core的信息添加到solr.xml里
		CoreAdminRequest.createCore(coreName, corePath.getPath(), server);
		CoreAdminRequest.reloadCore(coreName, server);
	}
}

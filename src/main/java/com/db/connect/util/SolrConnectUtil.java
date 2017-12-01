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
 * @time 2017��11��2������5:32:45
 */
public class SolrConnectUtil {
	static SolrClient solrConnect;
	
	private SolrConnectUtil() {
		this.solrConnect = null;
	}

	/**
	 * ����core
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
	 * �ر�solr����
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
	 * �����Ѵ��ڵ�core�����µ�core
	 * @param server 
	 * @param coreName ��core��
	 * @param defaultCore �Ѿ����ڵ�core
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public static void createNewCore(SolrClient server,String coreName,String defaultCore) throws SolrServerException, IOException {
		// ���solr.xml���úõ�cores��ΪĬ�ϣ����Ĭ��core��·��
		NamedList<Object> newCoreList = CoreAdminRequest.getStatus(coreName, server).getCoreStatus().get(coreName);
		if (newCoreList.size() > 0) {
			return;
		}
		NamedList<Object> list = CoreAdminRequest.getStatus(defaultCore, server).getCoreStatus().get(defaultCore);
		String path = (String) list.get("instanceDir");

		// ���solrhome,Ҳ����solr������������Ŀ¼
		String solrHome = path.substring(0, path.indexOf(defaultCore));

		// ������core�����ļ���
		File corePath = new File(solrHome + File.separator + coreName);
		if (!corePath.exists()) {
			corePath.mkdir();
		}
		// ������core�µ�conf�ļ���
		File confPath = new File(corePath.getAbsolutePath() + File.separator + "conf/");
		if (!confPath.exists()) {
			confPath.mkdir();
		}
		// ��Ĭ��core��conf���solrconfig.xml��schema.xml��������core��conf�¡��ⲽ�Ǳ����
		// ��Ϊ�½���core solr��ȥ��conf�ļ��������������ļ������û�оͻᱨ����core�򲻻ᴴ���ɹ�
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

		// ������core,ͬʱ�����core����Ϣ��ӵ�solr.xml��
		CoreAdminRequest.createCore(coreName, corePath.getPath(), server);
		CoreAdminRequest.reloadCore(coreName, server);
	}
}

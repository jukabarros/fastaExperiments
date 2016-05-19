package config;

import java.sql.Connection;
import java.util.Properties;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class ConnectCassandra {
	
	private Connection conn;
	private Cluster cluster;
	private Session session;
	
	public Connection connect(){
		try{
			Properties prop = ReadProperties.getProp();
			String node = prop.getProperty("cassandra.node");
			this.cluster = Cluster.builder()
			  .addContactPoint(node)
			  .build();
			this.cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(500000);
			
		}catch (Exception e){
			System.out.println("Erro ao se conectar com o BD: "+e.getMessage());
		}
		return conn;
	}
	
	public void getConnectionInfo(){
		this.connect();
		Metadata metadata = this.cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n", 
				metadata.getClusterName());
		for ( Host host : metadata.getAllHosts() ) {
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}
		this.close();
	}
	
	public void close() {
		   this.cluster.close();
		}
	
	// GET AND SET
	public Cluster getCluster() {
		return cluster;
	}

	public Session getSession() {
		return session;
	}

	public void setSession(Session session) {
		this.session = session;
	}

}

package config;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;

public class SimpleCassandraClientConnect {

	private Cluster cluster;
	private Metadata metadata;

	public void connect(String node) {
		cluster = Cluster.builder()
				.addContactPoints(node)
				.build();
		metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n", 
				metadata.getClusterName());
		for ( Host host : metadata.getAllHosts() ) {
			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}
	}

	public void close() {
		cluster.close();
	}
	public static void main(String[] args) {
		SimpleCassandraClientConnect scc = new SimpleCassandraClientConnect();
		scc.connect("150.161.11.134");
		scc.close();

	}

}

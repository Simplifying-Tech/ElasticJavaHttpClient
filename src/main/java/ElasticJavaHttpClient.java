import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.cluster.HealthResponse;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

public class ElasticJavaHttpClient {
	
	public static void main(String[] args) throws IOException {
		ElasticsearchClient httpClient =  getElasticsearchclient();
		
		//createIndex(httpClient);
		//createDocuments(httpClient);
		 getDocuments(httpClient);
	}
	private static void getDocuments(ElasticsearchClient httpClient) throws IOException {
        GetResponse<Employee> getResponse = httpClient.get(g -> g
                .index("employee_idx") 
                .id("101"),
                Employee.class      
            );
        Employee emp = getResponse.source();
 
        System.out.println("\n" + emp.getEmplId());
        System.out.println(emp.getName());
        System.out.println(emp.getDept());
    }
	private static void createDocuments(ElasticsearchClient httpClient) throws IOException {
        Employee employee = new Employee();
        employee.setEmplId(201);
        employee.setName("Alan Donald");
        employee.setDept("Sports");
 
        IndexResponse response = httpClient.index(i -> i
                .index("employee_idx")
                .id(employee.getEmplId().toString())
                .document(employee)
            );
 
        System.out.println("\n" + "Indexed with version " + response.version());
    }
	
	private static void createIndex(ElasticsearchClient httpClient) throws IOException {
        httpClient.indices().create(c -> c
                .index("employee_idx")
            );
    }
	
	private static ElasticsearchClient getElasticsearchclient() throws IOException {
        BasicCredentialsProvider credsProv = new BasicCredentialsProvider(); 
        credsProv.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "NlfGhKDDy=36FpvM*o0l"));
         
        RestClient restClient = RestClient
                .builder(new HttpHost("localhost", 9200, "http")) 
                .setHttpClientConfigCallback(hc -> hc
                    .setDefaultCredentialsProvider(credsProv)
                )
                .build();
         
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        ElasticsearchClient client = new ElasticsearchClient(transport);
         
        //to check Elastic server health and connection status
        HealthResponse healthResponse = client.cluster().health();
        System.out.printf("Elasticsearch status is: [%s]", healthResponse.status());
        return client;
    }

}

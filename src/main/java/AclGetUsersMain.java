import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CursorMarkParams;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Optional;

/**
 * Gets the users in the ACL collection.
 */
public class AclGetUsersMain {
  @SuppressWarnings(value = "unused")
  @Option(name = "-solrZkHosts", usage = "The zookeeper hosts string for the solr access control", required = true)
  private String solrZkHosts;

  @SuppressWarnings(value = "unused")
  @Option(name = "-solrZkChroot", usage = "The zookeeper chroot string for the solr access control", required = true)
  private String solrZkChroot;

  @SuppressWarnings(value = "unused")
  @Option(name = "-aclCollection", usage = "The acl collection", required = true)
  private String aclCollection;

  @SuppressWarnings(value = "unused")
  @Option(name = "-outputCsvFile", usage = "The output csv file to save the user IDs.", required = true)
  private String outputCsvFile;

  @SuppressWarnings(value = "unused")
  @Option(name = "-userQuery", usage = "The query to get users")
  private String userQuery = "type_s:user";


  public static void main(String [] args) throws Exception {
    AclGetUsersMain main = new AclGetUsersMain();
    CmdLineParser parser = new CmdLineParser(main);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      parser.printUsage(System.out);
      System.out.println(e.getLocalizedMessage());
      throw e;
    }
    main.export();
  }

  private void export() throws Exception {
    try (CloudSolrClient solrClient = new CloudSolrClient.Builder(Arrays.asList(solrZkHosts), Optional.of(solrZkChroot))
        .withConnectionTimeout(10000)
        .withSocketTimeout(60000)
        .build()) {
      try (FileWriter fw = new FileWriter(outputCsvFile);
           BufferedWriter bw = new BufferedWriter(fw)) {
        SolrQuery query = new SolrQuery();
        query.set("q", userQuery);
        query.setRows(10000);
        query.set("fl", "id");
        query.setSort(SolrQuery.SortClause.asc("id"));
        String cursorMark = CursorMarkParams.CURSOR_MARK_START;
        boolean done = false;
        while (!done) {
          query.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
          QueryResponse qr = solrClient.query(aclCollection, query);
          String nextCursorMark = qr.getNextCursorMark();
          for (SolrDocument sd : qr.getResults()) {
            bw.write(String.format("id:%s%n", sd.getFirstValue("id")));
          }
          if (cursorMark.equals(nextCursorMark)) {
            done = true;
          }
          cursorMark = nextCursorMark;
        }
      }
    }
  }
}

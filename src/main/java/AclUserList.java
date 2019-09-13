import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CursorMarkParams;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Collection;

public class AclUserList {
  @Option(name = "-solrZkHosts", usage = "The zookeeper hosts string for the solr access control", required = true)
  private String solrZkHosts;

  @Option(name = "-solrZkChroot", usage = "The zookeeper chroot string for the solr access control", required = true)
  private String solrZkChroot;

  @Option(name = "-aclCollection", usage = "The acl collection", required = true)
  private String aclCollection;

  @Option(name = "-outputCsvFile", usage = "The output csv file", required = true)
  private String outputCsvFile;

  public static void main(String [] args) throws Exception {
    AclUserList main = new AclUserList();
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
    try (CloudSolrClient solrClient = new CloudSolrClient.Builder()
        .withZkHost(solrZkHosts)
        .withZkChroot(solrZkChroot)
        .withConnectionTimeout(10000)
        .withSocketTimeout(60000)
        .build()) {
      try (FileWriter fw = new FileWriter(outputCsvFile + ".ids_only");
           BufferedWriter bw = new BufferedWriter(fw)) {
        SolrQuery query = new SolrQuery();
        query.set("q", "type_s:user");
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
            //bw.write(String.format("%s%n", sd.getFirstValue("id")));
            bw.write(String.format("{!graph from=\"inbound_ss\" to=\"outbound_ss\"}id:%s%n", ClientUtils.escapeQueryChars((String)sd.getFirstValue("id"))));
          }
          if (cursorMark.equals(nextCursorMark)) {
            done = true;
          }
          cursorMark = nextCursorMark;
        }
      }
      try (FileReader fileReader = new FileReader(outputCsvFile + ".ids_only");
           LineIterator lineIterator = new LineIterator(fileReader);
           FileWriter fw = new FileWriter(outputCsvFile);
           BufferedWriter bw = new BufferedWriter(fw)) {
        while (lineIterator.hasNext()) {
          String nextsGraphQuery = lineIterator.nextLine();
          SolrQuery query = new SolrQuery();
          query.set("q", nextsGraphQuery);
          query.setRows(1);
          query.set("fl", "id");
          System.out.println(nextsGraphQuery);
          QueryResponse qr = solrClient.query(aclCollection, query);
          if (qr.getResults().isEmpty()) {
            System.out.println("No match for " + nextsGraphQuery);
          }
          SolrDocument entries = qr.getResults().get(0);
          Collection<Object> ids = entries.getFieldValues("id");
          bw.write(String.format("%s\t%d\t%s%n", nextsGraphQuery, ids.size(), StringUtils.join(ids, ",")));
        }
      }
    }
  }
}

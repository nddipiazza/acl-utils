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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class AclGetGroupsMain {
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
    @Option(name = "-usersCsvFile", usage = "The file user IDs on each line containing the user IDs we want to get the groups of.", required = true)
    private String usersCsvFile;

    @SuppressWarnings(value = "unused")
    @Option(name = "-outputCsvFile", usage = "The output csv file which contains the users and the groups those users are a part of.", required = true)
    private String outputCsvFile;

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        AclGetGroupsMain main = new AclGetGroupsMain();
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
        int count = 0;
        try (CloudSolrClient solrClient = new CloudSolrClient.Builder(Arrays.asList(solrZkHosts), Optional.of(solrZkChroot))
                .withConnectionTimeout(10000)
                .withSocketTimeout(60000)
                .build();
             FileReader fileReader = new FileReader(usersCsvFile);
             LineIterator lineIterator = new LineIterator(fileReader);
             FileWriter fw = new FileWriter(outputCsvFile);
             BufferedWriter bw = new BufferedWriter(fw)) {
            while (lineIterator.hasNext()) {
                if (++count % 100 == 0) {
                    System.out.println("On count " + count);
                }
                long startedOn = System.currentTimeMillis();
                String nextUserId = lineIterator.nextLine();
                String nextGraphQuery = String.format("{!graph from=\"inbound_ss\" to=\"outbound_ss\"}id:%s%n", ClientUtils.escapeQueryChars(nextUserId));
                SolrQuery query = new SolrQuery();
                query.set("q", nextGraphQuery);
                query.set("fl", "id");
                query.setRows(10000);
                query.setSort(SolrQuery.SortClause.asc("id"));
                Set<String> ids = new HashSet<>();
                String cursorMark = CursorMarkParams.CURSOR_MARK_START;
                boolean done = false;
                while (!done) {
                    query.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
                    QueryResponse qr = solrClient.query(aclCollection, query);
                    String nextCursorMark = qr.getNextCursorMark();
                    for (SolrDocument sd : qr.getResults()) {
                        ids.add((String) sd.getFirstValue("id"));
                    }
                    if (cursorMark.equals(nextCursorMark)) {
                        done = true;
                    }
                    cursorMark = nextCursorMark;
                }


                if (ids.isEmpty()) {
                    System.out.println("No match for " + nextGraphQuery);
                }
                bw.write(String.format("%s\t%d\t%d\t%s%n", nextGraphQuery, ids.size(), System.currentTimeMillis() - startedOn, StringUtils.join(ids, ",")));
            }
        }
    }
}

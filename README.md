# ACL utils for graph query filters in Lucidworks Fusion

Get a list of all users in the acl collection

```
java -cp build/libs/acl-utils-farjar-1.0-SNAPSHOT.jar AclGUsersMain -solrZkHosts localhost:9983 -solrZkChroot /lwfusion/4.1.0/solr -aclCollection acl -outputCsvFile build/out.csv
```

Get all the nested acls for users

```
java -cp build/libs/acl-utils-farjar-1.0-SNAPSHOT.jar AclGetGroupsMain -solrZkHosts localhost:9983 -solrZkChroot /lwfusion/4.1.0/solr -aclCollection acl -usersCsvFile build/out.csv -outputCsvFile build/groups.csv
```
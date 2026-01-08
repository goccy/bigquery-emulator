package server_test

import (
	"context"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func TestComplexQuery(t *testing.T) {
	ctx := context.Background()
	const (
		projectName = "test"
		datasetName = "test"
	)

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(projectName, types.NewDataset(datasetName))
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Create tables
	adBaseObjectsTable := client.Dataset(datasetName).Table("ad_base_objects_1767790206")
	if err := adBaseObjectsTable.Create(ctx, &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			{Name: "guid", Type: bigquery.StringFieldType},
			{Name: "dn", Type: bigquery.StringFieldType},
			{Name: "name", Type: bigquery.StringFieldType},
			{Name: "display_name", Type: bigquery.StringFieldType},
			{Name: "sam_account_name", Type: bigquery.StringFieldType},
			{Name: "ou", Type: bigquery.StringFieldType},
			{Name: "upn", Type: bigquery.StringFieldType},
			{Name: "user_account_control", Type: bigquery.IntegerFieldType},
			{Name: "sid", Type: bigquery.StringFieldType},
			{Name: "primary_group_id", Type: bigquery.IntegerFieldType},
			{Name: "last_logon_timestamp", Type: bigquery.TimestampFieldType},
			{Name: "os", Type: bigquery.StringFieldType},
			{Name: "os_ver", Type: bigquery.StringFieldType},
			{Name: "os_sp", Type: bigquery.StringFieldType},
			{Name: "dns_host_name", Type: bigquery.StringFieldType},
			{Name: "email", Type: bigquery.StringFieldType},
			{Name: "location", Type: bigquery.StringFieldType},
			{Name: "department", Type: bigquery.StringFieldType},
			{Name: "member_of", Type: bigquery.StringFieldType, Repeated: true},
			{Name: "last_modified", Type: bigquery.TimestampFieldType},
			{Name: "domain_name", Type: bigquery.StringFieldType},
			{Name: "netbios", Type: bigquery.StringFieldType},
			{Name: "type", Type: bigquery.StringFieldType},
			{Name: "agent_id", Type: bigquery.StringFieldType},
			{Name: "security_groups", Type: bigquery.StringFieldType, Repeated: true},
			{Name: "generatedTime", Type: bigquery.TimestampFieldType},
			{Name: "sid_history", Type: bigquery.StringFieldType, Repeated: true},
			{Name: "service_principal_names", Type: bigquery.StringFieldType, Repeated: true},
			{Name: "msds_allowed_to_delegate_to", Type: bigquery.StringFieldType, Repeated: true},
			{Name: "msds_allowed_to_act_on_behalf_of_other_identity", Type: bigquery.StringFieldType},
			{Name: "msds_supported_encryption_types", Type: bigquery.IntegerFieldType},
			{Name: "source", Type: bigquery.StringFieldType},
			{Name: "netbios_and_sam_account_name", Type: bigquery.StringFieldType},
			{Name: "title", Type: bigquery.StringFieldType},
			{Name: "user_type", Type: bigquery.StringFieldType},
			{Name: "company_name", Type: bigquery.StringFieldType},
			{Name: "country", Type: bigquery.StringFieldType},
			{Name: "preferred_language", Type: bigquery.StringFieldType},
			{Name: "other_mails", Type: bigquery.StringFieldType, Repeated: true},
			{Name: "proxy_addresses", Type: bigquery.StringFieldType, Repeated: true},
			{Name: "sign_in_sessions_valid_from", Type: bigquery.TimestampFieldType},
			{Name: "manager_raw", Type: bigquery.StringFieldType},
			{Name: "manager_sam_account_name", Type: bigquery.StringFieldType},
			{Name: "manager_domain", Type: bigquery.StringFieldType},
			{Name: "manager_ou", Type: bigquery.StringFieldType},
			{Name: "on_prem_last_sync", Type: bigquery.TimestampFieldType},
			{Name: "on_prem_sync_enabled", Type: bigquery.BooleanFieldType},
			{Name: "on_prem_sam_account_name", Type: bigquery.StringFieldType},
			{Name: "on_prem_sid", Type: bigquery.StringFieldType},
			{Name: "on_prem_dn_raw", Type: bigquery.StringFieldType},
			{Name: "on_prem_dn_cn", Type: bigquery.StringFieldType},
			{Name: "on_prem_dn_domain", Type: bigquery.StringFieldType},
			{Name: "on_prem_dn_ou", Type: bigquery.StringFieldType},
			{Name: "on_prem_extension_attributes", Type: bigquery.StringFieldType}, // Assuming JSON string or similar
			{Name: "pwd_last_change", Type: bigquery.TimestampFieldType},
			{Name: "pwd_policies", Type: bigquery.StringFieldType}, // Assuming JSON string or similar
			{Name: "pwd_profile", Type: bigquery.StringFieldType},  // Assuming JSON string or similar
			{Name: "admin_count", Type: bigquery.IntegerFieldType},
			{Name: "aad_roles", Type: bigquery.StringFieldType, Repeated: true},
			{Name: "employee_leave_date", Type: bigquery.TimestampFieldType},
			{Name: "is_managed", Type: bigquery.BooleanFieldType},
			{Name: "trust_type", Type: bigquery.StringFieldType},
			{Name: "when_created", Type: bigquery.TimestampFieldType},
			{Name: "_o365_specific_fields", Type: bigquery.StringFieldType}, // Assuming JSON string or similar
			{Name: "onPremisesImmutableId", Type: bigquery.StringFieldType},
			{Name: "directory_id", Type: bigquery.StringFieldType},
			{Name: "clean_domain_name", Type: bigquery.StringFieldType},
			{Name: "trimmed_domain_name", Type: bigquery.StringFieldType},
			{Name: "clean_trimmed_domain_name", Type: bigquery.StringFieldType},
		},
	}); err != nil {
		t.Fatal(err)
	}

	adRelationsTable := client.Dataset(datasetName).Table("ad_relations_view")
	if err := adRelationsTable.Create(ctx, &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			{Name: "guid", Type: bigquery.StringFieldType},
			{Name: "owner_guid", Type: bigquery.StringFieldType},
		},
	}); err != nil {
		t.Fatal(err)
	}

	adBucketsTable := client.Dataset(datasetName).Table("ad_buckets_view")
	if err := adBucketsTable.Create(ctx, &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			{Name: "guid", Type: bigquery.StringFieldType},
			{Name: "dn", Type: bigquery.StringFieldType},
			{Name: "group_type", Type: bigquery.StringFieldType},
		},
	}); err != nil {
		t.Fatal(err)
	}

	adUserRolesTable := client.Dataset(datasetName).Table("ad_user_roles_view")
	if err := adUserRolesTable.Create(ctx, &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			{Name: "guid", Type: bigquery.StringFieldType},
			{Name: "role", Type: bigquery.StringFieldType},
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Insert dummy data
	type AdBaseObject struct {
		Guid       string `bigquery:"guid"`
		Dn         string `bigquery:"dn"`
		Source     string `bigquery:"source"`
		DomainName string `bigquery:"domain_name"`
	}
	if err := adBaseObjectsTable.Inserter().Put(ctx, []*AdBaseObject{
		{
			Guid:       "guid1",
			Dn:         "dn1",
			Source:     "source1",
			DomainName: "domain1",
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Execute the complex query
	query := client.Query(`
SELECT 
    select_query.guid AS guid, 
    ANY_VALUE(select_query.dn) AS dn, 
    ANY_VALUE(select_query.name) AS name, 
    ANY_VALUE(select_query.display_name) AS display_name, 
    ANY_VALUE(select_query.sam_account_name) AS sam_account_name, 
    ANY_VALUE(select_query.ou) AS ou, 
    ANY_VALUE(select_query.upn) AS upn, 
    ANY_VALUE(select_query.user_account_control) AS user_account_control, 
    ANY_VALUE(select_query.sid) AS sid, 
    ANY_VALUE(select_query.primary_group_id) AS primary_group_id, 
    ANY_VALUE(select_query.last_logon_timestamp) AS last_logon_timestamp, 
    ANY_VALUE(select_query.os) AS os, 
    ANY_VALUE(select_query.os_ver) AS os_ver, 
    ANY_VALUE(select_query.os_sp) AS os_sp, 
    ANY_VALUE(select_query.dns_host_name) AS dns_host_name, 
    ANY_VALUE(select_query.email) AS email, 
    ANY_VALUE(select_query.location) AS location, 
    ANY_VALUE(select_query.department) AS department, 
    ANY_VALUE(select_query.member_of) AS member_of, 
    ANY_VALUE(select_query.last_modified) AS last_modified, 
    ANY_VALUE(select_query.domain_name) AS domain_name, 
    ANY_VALUE(select_query.netbios) AS netbios, 
    ANY_VALUE(select_query.type) AS type, 
    ANY_VALUE(select_query.agent_id) AS agent_id, 
    ANY_VALUE(select_query.generatedTime) AS generatedTime, 
    ANY_VALUE(select_query.sid_history) AS sid_history, 
    ANY_VALUE(select_query.service_principal_names) AS service_principal_names, 
    ANY_VALUE(select_query.msds_allowed_to_delegate_to) AS msds_allowed_to_delegate_to, 
    ANY_VALUE(select_query.msds_allowed_to_act_on_behalf_of_other_identity) AS msds_allowed_to_act_on_behalf_of_other_identity, 
    ANY_VALUE(select_query.msds_supported_encryption_types) AS msds_supported_encryption_types, 
    ANY_VALUE(select_query.source) AS source, 
    ANY_VALUE(select_query.netbios_and_sam_account_name) AS netbios_and_sam_account_name, 
    ANY_VALUE(select_query.title) AS title, 
    ANY_VALUE(select_query.user_type) AS user_type, 
    ANY_VALUE(select_query.company_name) AS company_name, 
    ANY_VALUE(select_query.country) AS country, 
    ANY_VALUE(select_query.preferred_language) AS preferred_language, 
    ANY_VALUE(select_query.other_mails) AS other_mails, 
    ANY_VALUE(select_query.proxy_addresses) AS proxy_addresses, 
    ANY_VALUE(select_query.sign_in_sessions_valid_from) AS sign_in_sessions_valid_from, 
    ANY_VALUE(select_query.manager_raw) AS manager_raw, 
    ANY_VALUE(select_query.manager_sam_account_name) AS manager_sam_account_name, 
    ANY_VALUE(select_query.manager_domain) AS manager_domain, 
    ANY_VALUE(select_query.manager_ou) AS manager_ou, 
    ANY_VALUE(select_query.on_prem_last_sync) AS on_prem_last_sync, 
    ANY_VALUE(select_query.on_prem_sync_enabled) AS on_prem_sync_enabled, 
    ANY_VALUE(select_query.on_prem_sam_account_name) AS on_prem_sam_account_name, 
    ANY_VALUE(select_query.on_prem_sid) AS on_prem_sid, 
    ANY_VALUE(select_query.on_prem_dn_raw) AS on_prem_dn_raw, 
    ANY_VALUE(select_query.on_prem_dn_cn) AS on_prem_dn_cn, 
    ANY_VALUE(select_query.on_prem_dn_domain) AS on_prem_dn_domain, 
    ANY_VALUE(select_query.on_prem_dn_ou) AS on_prem_dn_ou, 
    ANY_VALUE(select_query.on_prem_extension_attributes) AS on_prem_extension_attributes, 
    ANY_VALUE(select_query.pwd_last_change) AS pwd_last_change, 
    ANY_VALUE(select_query.pwd_policies) AS pwd_policies, 
    ANY_VALUE(select_query.pwd_profile) AS pwd_profile, 
    ANY_VALUE(select_query.admin_count) AS admin_count, 
    ANY_VALUE(select_query.employee_leave_date) AS employee_leave_date, 
    ANY_VALUE(select_query.is_managed) AS is_managed, 
    ANY_VALUE(select_query.trust_type) AS trust_type, 
    ANY_VALUE(select_query.when_created) AS when_created, 
    ANY_VALUE(select_query._o365_specific_fields) AS _o365_specific_fields, 
    ANY_VALUE(select_query.onPremisesImmutableId) AS onPremisesImmutableId, 
    ANY_VALUE(select_query.directory_id) AS directory_id, 
    ANY_VALUE(select_query.clean_domain_name) AS clean_domain_name, 
    ANY_VALUE(select_query.trimmed_domain_name) AS trimmed_domain_name, 
    ANY_VALUE(select_query.clean_trimmed_domain_name) AS clean_trimmed_domain_name, 
    ARRAY_AGG(distinct(ad_buckets_view.dn) IGNORE NULLS) AS security_groups,
    ARRAY_AGG(distinct(ad_user_roles_view.role) IGNORE NULLS) AS aad_roles
FROM (
    SELECT
        ad_base_objects_1767790206.guid AS guid,
        ad_base_objects_1767790206.dn AS dn,
        ad_base_objects_1767790206.name AS name,
        ad_base_objects_1767790206.display_name AS display_name,
        ad_base_objects_1767790206.sam_account_name AS sam_account_name,
        ad_base_objects_1767790206.ou AS ou,
        ad_base_objects_1767790206.upn AS upn,
        ad_base_objects_1767790206.user_account_control AS user_account_control,
        ad_base_objects_1767790206.sid AS sid,
        ad_base_objects_1767790206.primary_group_id AS primary_group_id,
        ad_base_objects_1767790206.last_logon_timestamp AS last_logon_timestamp,
        ad_base_objects_1767790206.os AS os,
        ad_base_objects_1767790206.os_ver AS os_ver,
        ad_base_objects_1767790206.os_sp AS os_sp,
        ad_base_objects_1767790206.dns_host_name AS dns_host_name,
        ad_base_objects_1767790206.email AS email,
        ad_base_objects_1767790206.location AS location,
        ad_base_objects_1767790206.department AS department,
        ad_base_objects_1767790206.member_of AS member_of,
        ad_base_objects_1767790206.last_modified AS last_modified,
        ad_base_objects_1767790206.domain_name AS domain_name,
        ad_base_objects_1767790206.netbios AS netbios,
        ad_base_objects_1767790206.type AS type,
        ad_base_objects_1767790206.agent_id AS agent_id,
        ad_base_objects_1767790206.security_groups AS security_groups,
        ad_base_objects_1767790206.generatedTime AS generatedTime,
        ad_base_objects_1767790206.sid_history AS sid_history,
        ad_base_objects_1767790206.service_principal_names AS service_principal_names,
        ad_base_objects_1767790206.msds_allowed_to_delegate_to AS msds_allowed_to_delegate_to,
        ad_base_objects_1767790206.msds_allowed_to_act_on_behalf_of_other_identity AS msds_allowed_to_act_on_behalf_of_other_identity,
        ad_base_objects_1767790206.msds_supported_encryption_types AS msds_supported_encryption_types,
        ad_base_objects_1767790206.source AS source,
        ad_base_objects_1767790206.netbios_and_sam_account_name AS netbios_and_sam_account_name,
        ad_base_objects_1767790206.title AS title,
        ad_base_objects_1767790206.user_type AS user_type,
        ad_base_objects_1767790206.company_name AS company_name,
        ad_base_objects_1767790206.country AS country,
        ad_base_objects_1767790206.preferred_language AS preferred_language,
        ad_base_objects_1767790206.other_mails AS other_mails,
        ad_base_objects_1767790206.proxy_addresses AS proxy_addresses,
        ad_base_objects_1767790206.sign_in_sessions_valid_from AS sign_in_sessions_valid_from,
        ad_base_objects_1767790206.manager_raw AS manager_raw,
        ad_base_objects_1767790206.manager_sam_account_name AS manager_sam_account_name,
        ad_base_objects_1767790206.manager_domain AS manager_domain,
        ad_base_objects_1767790206.manager_ou AS manager_ou,
        ad_base_objects_1767790206.on_prem_last_sync AS on_prem_last_sync,
        ad_base_objects_1767790206.on_prem_sync_enabled AS on_prem_sync_enabled,
        ad_base_objects_1767790206.on_prem_sam_account_name AS on_prem_sam_account_name,
        ad_base_objects_1767790206.on_prem_sid AS on_prem_sid,
        ad_base_objects_1767790206.on_prem_dn_raw AS on_prem_dn_raw,
        ad_base_objects_1767790206.on_prem_dn_cn AS on_prem_dn_cn,
        ad_base_objects_1767790206.on_prem_dn_domain AS on_prem_dn_domain,
        ad_base_objects_1767790206.on_prem_dn_ou AS on_prem_dn_ou,
        ad_base_objects_1767790206.on_prem_extension_attributes AS on_prem_extension_attributes,
        ad_base_objects_1767790206.pwd_last_change AS pwd_last_change,
        ad_base_objects_1767790206.pwd_policies AS pwd_policies,
        ad_base_objects_1767790206.pwd_profile AS pwd_profile,
        ad_base_objects_1767790206.admin_count AS admin_count,
        ad_base_objects_1767790206.aad_roles AS aad_roles,
        ad_base_objects_1767790206.employee_leave_date AS employee_leave_date,
        ad_base_objects_1767790206.is_managed AS is_managed,
        ad_base_objects_1767790206.trust_type AS trust_type,
        ad_base_objects_1767790206.when_created AS when_created,
        ad_base_objects_1767790206._o365_specific_fields AS _o365_specific_fields,
        ad_base_objects_1767790206.onPremisesImmutableId AS onPremisesImmutableId,
        ad_base_objects_1767790206.directory_id AS directory_id,
        ad_base_objects_1767790206.clean_domain_name AS clean_domain_name,
        ad_base_objects_1767790206.trimmed_domain_name AS trimmed_domain_name,
        ad_base_objects_1767790206.clean_trimmed_domain_name AS clean_trimmed_domain_name
    FROM test.test.ad_base_objects_1767790206
) AS select_query
LEFT OUTER JOIN test.test.ad_relations_view ON select_query.guid = ad_relations_view.guid
LEFT OUTER JOIN test.test.ad_buckets_view ON ad_relations_view.owner_guid = ad_buckets_view.guid AND ad_buckets_view.group_type = @group_type_1
LEFT OUTER JOIN test.test.ad_user_roles_view ON select_query.guid = ad_user_roles_view.guid
WHERE (select_query.source NOT IN UNNEST(@source_1)) AND (select_query.domain_name NOT IN UNNEST(@domain_name_1)) 
GROUP BY select_query.guid
`)

	query.Parameters = []bigquery.QueryParameter{
		{Name: "group_type_1", Value: "some_group_type"},
		{Name: "source_1", Value: []string{"excluded_source"}},
		{Name: "domain_name_1", Value: []string{"excluded_domain"}},
	}

	resultsTable := client.Dataset(datasetName).Table("results_table")
	query.QueryConfig.Dst = resultsTable
	query.QueryConfig.WriteDisposition = bigquery.WriteTruncate

	job, err := query.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := status.Err(); err != nil {
		t.Fatal(err)
	}

	// Create a view that looks at this table
	view := client.Dataset(datasetName).Table("results_view")
	if err := view.Create(ctx, &bigquery.TableMetadata{
		ViewQuery: "SELECT * FROM `" + projectName + "." + datasetName + ".results_table`",
	}); err != nil {
		t.Fatal(err)
	}

	// Check it has the same amount of rows
	it := view.Read(ctx)
	var rowCount int
	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("failed to iterate at row %d: %v", rowCount, err)
		}
		rowCount++
	}
	if rowCount != 1 {
		t.Fatalf("expected 1 row in view, got %d", rowCount)
	}
}

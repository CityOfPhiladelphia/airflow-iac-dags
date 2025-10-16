from airflow.providers.postgres.hooks.postgres import PostgresHook

# import psycopg2
import psycopg2.extras
# import json


def set_viewer_privileges(
    share_privileges, account_name, table_name, postgres_conn_id, viewer_account
):
    # In case viewer_account differs from account_name
    owning_dept = "_".join(viewer_account.split("_")[1:])

    ######################
    # Connect to Postgres
    db_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    pg_conn = db_hook.get_conn()
    # No autocommit so we can modify privs seamlessly in one transaction.
    pg_conn.autocommit = False
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # 1. assert table exists
    stmt = f"SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = '{viewer_account}' AND tablename  = '{table_name}');"
    pg_cur.execute(stmt)
    exists = pg_cur.fetchone()
    if not exists:
        raise AssertionError("Table doesnt exist! Why is this task running?")

    ######################
    # 1. Find existing users with privileges
    stmt = f"""
    SELECT distinct grantee 
    FROM information_schema.role_table_grants 
    WHERE table_name='{table_name}'
    and table_schema='{viewer_account}';
    """
    pg_cur.execute(stmt)
    grantees_dict = pg_cur.fetchall()

    # compile list of who has granted rights.
    current_grantees_list = []
    if grantees_dict:
        for grantee in grantees_dict:
            # Access the grantee key from the returned list of RealDictRow objects
            if grantee["grantee"] == "postgres":
                pass
            else:
                current_grantees_list.append(grantee["grantee"])

    # Compile list of people who should have access to this table.
    # share_privileges indicates we want only specific departments to have access.
    wanted_grantees_list = []
    if share_privileges:
        for dept in share_privileges.split(","):
            if dept == "ago":
                wanted_grantees_list.append("ago")
            # If enterprise shared, share to everyone via special departments role, so they all can select.
            elif dept == "enterprise":
                wanted_grantees_list.append("departments")
            else:
                wanted_grantees_list.append(dept)
                wanted_grantees_list.append(dept + "_read")

    # These should always have read access
    wanted_grantees_list.append(owning_dept)
    wanted_grantees_list.append(account_name)
    wanted_grantees_list.append(account_name + "_read")
    wanted_grantees_list.append("viewer")
    wanted_grantees_list.append("db2_superuser")
    wanted_grantees_list.append(viewer_account)
    wanted_grantees_list.append(viewer_account.replace("viewer_", ""))
    wanted_grantees_list.append(viewer_account.replace("viewer_", "") + "_read")

    # Special sensitive departments that shouldn't have db2_admin access
    if viewer_account.replace("viewer_", "") not in ["cityeo_onephilly"]:
        wanted_grantees_list.append("db2_admin")

    # Remove dupes, which happens a lot because my code sucks.
    wanted_grantees_list = list(set(wanted_grantees_list))
    print(f"Wanted select grantees list: {wanted_grantees_list}")

    # Grantees that should have all privileges
    all_priv_grantees = ["viewer", "db2_superuser", viewer_account]

    # Now make our update statements. First check for things that should be revoked.
    privilege_update_stmt = []
    for c in current_grantees_list:
        if c not in wanted_grantees_list:
            privilege_update_stmt.append(
                f"REVOKE ALL PRIVILEGES ON TABLE {viewer_account}.{table_name} FROM {c};"
            )

    for w in wanted_grantees_list:
        if w not in current_grantees_list and w not in all_priv_grantees:
            privilege_update_stmt.append(
                f"GRANT SELECT ON TABLE {viewer_account}.{table_name} TO {w};"
            )

    for a in all_priv_grantees:
        if a not in current_grantees_list:
            privilege_update_stmt.append(
                f"GRANT ALL PRIVILEGES ON TABLE {viewer_account}.{table_name} TO {a};"
            )

    # Make sure owner is set properly
    find_owner_stmt = f"SELECT tableowner FROM pg_catalog.pg_tables WHERE schemaname = '{viewer_account}' AND tablename = '{table_name}';"
    pg_cur.execute(find_owner_stmt)
    current_owner = pg_cur.fetchone()["tableowner"]

    print(f"Current owner: {current_owner}")
    if current_owner != viewer_account:
        privilege_update_stmt.append(
            f"ALTER TABLE {viewer_account}.{table_name} OWNER TO {viewer_account};"
        )

    if not privilege_update_stmt:
        print("Privileges don't need modification.")
    else:
        print("Devised privilege update stmt:")
        for s in privilege_update_stmt:
            print(s)
            pg_cur.execute(s)
            pg_cur.execute("COMMIT;")

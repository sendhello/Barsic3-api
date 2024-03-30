#!/bin/bash
echo "======================== Start restore database ========================"
ls /var/opt/mssql/backup

for DB_NAME in AquaPark_Ulyanovsk Beach bitrix_transaction RK7
do
  echo "Restore database" $DB_NAME
  /opt/mssql-tools/bin/sqlcmd \
    -S mssql -U sa -P MyPass@word \
    -Q "RESTORE DATABASE ${DB_NAME} FROM DISK = \"/var/opt/backup/${DB_NAME}.bak\" WITH MOVE \"${DB_NAME}\" TO \"/var/opt/mssql/data/${DB_NAME}.mdf\", MOVE \"${DB_NAME}_log\" TO \"/var/opt/mssql/data/${DB_NAME}_log.ldf\""
done

echo "========================= Restore databases done! ========================="
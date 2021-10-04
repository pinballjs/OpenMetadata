package org.openmetadata.catalog.jdbi3;

import org.skife.jdbi.v2.sqlobject.CreateSqlObject;

public abstract class OperationRepository {
    @CreateSqlObject
    abstract OperationDAO operationDAO();

    public interface OperationDAO {

    }
}

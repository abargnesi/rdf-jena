package com.github.rdf_jena;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;

import java.util.function.Function;

public final class TransactionUtil {

    static public <U> U executeInTransaction(Dataset dataset, ReadWrite type, Function<Dataset, U> transactionBody) {
        if (dataset.isInTransaction() || !dataset.supportsTransactions()) {
            return transactionBody.apply(dataset);
        } else {
            dataset.begin(type);
            try {
                return transactionBody.apply(dataset);
            } finally {
                dataset.commit();
                dataset.end();
            }
        }
    }

    private TransactionUtil() {
        // static accessors only
    }
}

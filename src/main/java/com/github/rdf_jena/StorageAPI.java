package com.github.rdf_jena;

import org.apache.jena.riot.Lang;

import java.io.InputStream;

public interface StorageAPI {

    void load(InputStream stream, Lang langHint);
}

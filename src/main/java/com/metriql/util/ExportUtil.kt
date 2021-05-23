package com.metriql.util

import com.metriql.db.QueryResult
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import org.apache.commons.csv.QuoteMode
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.PrintWriter
import javax.xml.bind.DatatypeConverter

object ExportUtil {
    fun exportAsCSV(result: QueryResult): ByteArray {
        if (result.metadata == null || result.result == null) {
            throw MetriqlExceptions.QUERY_NO_RESULT.exception
        }
        val out = ByteArrayOutputStream()
        val format = CSVFormat.DEFAULT.withQuoteMode(QuoteMode.NON_NUMERIC)
        val csvPrinter = CSVPrinter(PrintWriter(out), format)
        try {
            csvPrinter.printRecord(result.metadata!!.map { it.name })
            result.result!!.forEach {
                csvPrinter.printRecord(
                    it.map {
                        if (it is List<*> || it is Map<*, *>) {
                            JsonHelper.encode(it)
                        } else if (it is ByteArray) {
                            DatatypeConverter.printBase64Binary(it)
                        } else {
                            it
                        }
                    }
                )
            }
            csvPrinter.flush()
        } catch (e: IOException) {
            throw e
        }
        return out.toByteArray()
    }
}

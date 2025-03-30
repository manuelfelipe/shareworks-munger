package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Give this program some arguments!  It needs the name of an html file with your data to munge.\n")
	}
	someErrors := false
	for _, arg := range os.Args[1:] {
		// Parse the file and munge it.
		columns, entries, err := munge(arg)
		if err != nil {
			someErrors = true
			fmt.Fprintf(os.Stderr, "%q: failed: %s\n", arg, err)
			continue
		}
		// Emit csv.
		emitCsv(os.Stdout, columns, entries)
		// Done!
		fmt.Fprintf(os.Stderr, "%q: munged successfully: copy the above to a file (or use shell redirection) to save it.\n", arg)
	}
	if someErrors {
		os.Exit(14)
	}
}

func munge(filename string) (columns []string, entries []map[string]string, err error) {
	// Quick sanity check on the file type.
	if !strings.HasSuffix(filename, ".html") {
		return nil, nil, fmt.Errorf("not munging file %q; this tool works with html files (a '.html' suffix) only", filename)
	}

	// Pop 'er open.
	bs, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open html file %q: %w", filename, err)
	}
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(bs))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open html file %q: %w", filename, err)
	}

	// Check for the most likely data collection error and warn about it specifically.
	if doc.Find("iframe#transaction-statement-iframe").Length() > 0 {
		return nil, nil, fmt.Errorf("wrong html -- it looks like you got the enclosing document.  Check the README again -- did you do extraction correctly?  You have to get the content from inside the iframe element.  (Sorry this is complicated.  I didn't write the website.)")
	}

	// All the relevant data is in tables with this class.
	//  A lot of irrelevant data is too, but we'll sort that out later.
	tablesSelection := doc.Find("table.sw-datatable")
	if tablesSelection.Length() < 1 {
		return nil, nil, fmt.Errorf("found no shareworks data tables -- are you sure this is the right html?")
	}

	// Pluck out tables that have a header row that contains the text "Release".
	//  The "Release" tables are the only ones that are useful.
	//  (Other tables contain summaries, but the summaries are... basically useless, and exclude all of the facts that are actually relevant.  Amazing.)
	tablesSelection = tablesSelection.FilterFunction(func(i int, sel *goquery.Selection) bool {
		headerText := sel.Find("th.newReportTitleStyle").First().Text()
		return strings.Contains(headerText, "Release")
	})
	if tablesSelection.Length() < 1 {
		return nil, nil, fmt.Errorf("none of the shareworks data tables had titles containing the word 'Release' -- are you sure this is the right html?  We expected the events to all have 'Release' in the title somewhere.")
	}

	// BUT WAIT!  THERE'S MORE!
	// Look for h2 tags.  These contain the info about which kind of good we're handling.
	//  This is super important if you have more than one kind of stock or token being reported.
	//  Note that this information is NOT the actual stock or good itself -- it's the distribution schedule name.
	//   You'll have to demux that information back onto the actual stock or good manually with information in your hands as a human -- the document **literally** does not contain this information, as far as I can tell.
	// We have to do this in *the same query* as getting the tables, so that they're interleaved in the correct order in our selection here --
	//  the h2 tags aren't parents of the data they describe, they're just *before* the data they describe.  Additional "whee" for parsing :))))
	//   Can you imagine how great it would be if these tables actually say which unit they're denominated in?  But they don't :D :D :D :D
	//  So, that tablesSelection var earlier is demoted to just being another sanitychecker, and we'll loop over this below, looking for both tables and h2 tags.
	//   And we'll be re-doing the filter for tables-that-are-actually-relevant below, too.  Agghsdfhwefhsdfh.
	tablesAndHeadersSelection := doc.Find("h2, table.sw-datatable")

	// Okay, it's almost time to start accumulating data.
	// I'm gonna kinda try to normalize this to columnar as we go;
	//  and I'm not hard-coding any column headings,
	//   so, first encounter with a data entry in the whole document determins the order in which it will appear as a column.
	// See the definition of `columns` and `entries` at the top, in the function's returns.

	// We also need one slot of memory to remember the text of the last h2 tag we saw,
	//  because that's the distribution schedule name, and will apply to several rows, which we're about to loop over.
	var distributionScheduleName string

	// Go over the whole melange.
	// The headers become one column; the tables that are relevant each become one row in our sanitized data.
	// Yeah, one table becomes one row.  Yeah.  Yeahhhhh.
	// This is why your accountant didn't want to work with this format.  Because it's insane.  This is not how data should be formatted.
	// Anyway, let's go:
	tablesAndHeadersSelection.Each(func(i int, sel *goquery.Selection) {
		// First: see if this is:
		//  - a heading (e.g. might indicate which distribution schedule the following tables are for),
		//  - or if it's a table that we care about (e.g. it describes a distribution event),
		//  - or if it's one of the other tables that's useless (see earlier comments).
		// If it's a heading, we'll handle that in this logic block;
		// if it's a useless table, we'll skip out;
		// if it's a relevant table, the majority of the logic will continue below.
		switch {
		case sel.Is("h2"):
			distributionScheduleName = strings.TrimPrefix(strings.TrimSpace(sel.Text()), "Summary of ")
			return
		case sel.Is("table.sw-datatable"):
			headerText := sel.Find("th.newReportTitleStyle").First().Text()
			isRelease := strings.Contains(headerText, "Release")
			isWithdrawal := strings.Contains(headerText, "Withdrawal on")
			if !isRelease && !isWithdrawal {
				return
			}
			// if it contains either word, it's relevant: continue...
		default:
			panic("unreachable, earlier filter should not have matched this")
		}

		// Make some temporary memory to put this row's data in as we find it.
		row := map[string]string{}
		entries = append(entries, row)

		// Append the distributionScheduleName as a column.
		accumulate(&columns, row, "Distribution Schedule", distributionScheduleName)

		// Pick a title for the event.
		//  We'll use that same table header that we happened to already look at above to filter the tables in the first place.
		headerText := strings.TrimSpace(sel.Find("th.newReportTitleStyle").First().Text())
		accumulate(&columns, row, "Event", headerText)

		// Add the Type column
		if strings.Contains(headerText, "Release") {
			accumulate(&columns, row, "Type", "Buy")
		} else if strings.Contains(headerText, "Withdrawal on") {
			accumulate(&columns, row, "Type", "Sell")
		}

		// Some brain genius made a four-column layout: two columns of two paired columns.  KVKV.
		// So we get to suss that back out.  Neato.
		// They tend to read top-bottom and then top-bottom again, and I'm actually going to bother to parse that ordering.
		var col1, col2, col3, col4 []string
		sel.Find("tr").Each(func(i int, sel *goquery.Selection) {
			sel.Find("td.staticViewTableColumn1").Each(func(i int, sel *goquery.Selection) {
				if i%2 == 0 {
					col1 = append(col1, strings.TrimSpace(sel.Text()))
				} else {
					col3 = append(col3, strings.TrimSpace(sel.Text()))
				}
			})
			sel.Find("td.staticViewTableColumn2").Each(func(i int, sel *goquery.Selection) {
				if i%2 == 0 {
					col2 = append(col2, strings.TrimSpace(sel.Text()))
				} else {
					col4 = append(col4, strings.TrimSpace(sel.Text()))
				}
			})
		})
		for i := range col1 {
			accumulate(&columns, row, col1[i], col2[i])
		}
		for i := range col3 {
			accumulate(&columns, row, col3[i], col4[i])
		}

		// Process additional tables that follow the main table
		if strings.Contains(headerText, "Release") {
			// For releases, find and process the "Value of Shares Sold" table that follows
			nextTable := sel.Next()
			if nextTable.Length() > 0 && nextTable.Is("table.sw-datatable") {
				// Check if it's a "Value of Shares Sold" table
				headerText := nextTable.Find("th.newReportHeadingStyle").First().Text()
				if strings.TrimSpace(headerText) == "Value of Shares Sold" {
					processValueTable(nextTable, &columns, row)

					// Get the total value from the next table
					totalTable := nextTable.Next()
					if totalTable.Length() > 0 && totalTable.Is("table.sw-datatable") {
						totalText := totalTable.Find("td.defaultTableModelTextBold").First().Text()
						if strings.HasPrefix(totalText, "Total Value:") {
							accumulate(&columns, row, "Total Value", strings.TrimSpace(strings.TrimPrefix(totalText, "Total Value:")))
						}
					}
				}
			}
		} else if strings.Contains(headerText, "Withdrawal on") {
			// For withdrawals, process all the following tables until we hit a non-relevant one
			currentTable := sel.Next()
			for currentTable.Length() > 0 {
				if !currentTable.Is("table.sw-datatable") {
					break
				}

				headerText := currentTable.Find("th.newReportHeadingStyle, th.newReportTitleStyle").First().Text()
				if headerText == "" {
					currentTable = currentTable.Next()
					continue
				}
				headerText = strings.TrimSpace(headerText)

				// Process tables based on their headers
				switch headerText {
				case "Sale Breakdown", "Electronic Share Transfer", "Mail cash to broker", "Net Proceeds":
					processValueTable(currentTable, &columns, row)

					// Check for total value table
					totalTable := currentTable.Next()
					if totalTable.Length() > 0 && totalTable.Is("table.sw-datatable") {
						totalText := totalTable.Find("td.defaultTableModelTextBold").First().Text()
						if strings.HasPrefix(totalText, "Total Value:") {
							accumulate(&columns, row, headerText+" Total", strings.TrimSpace(strings.TrimPrefix(totalText, "Total Value:")))
							currentTable = totalTable.Next()
							continue
						}
					}
				}
				currentTable = currentTable.Next()
			}
		}
	})

	// Sort entries by Settlement Date
	sort.Slice(entries, func(i, j int) bool {
		date1, ok1 := entries[i]["Settlement Date"]
		date2, ok2 := entries[j]["Settlement Date"]

		// If either entry doesn't have a Settlement Date, keep original order
		if !ok1 || !ok2 {
			return false
		}

		// Parse dates in the format "02-Jan-2006"
		t1, err1 := time.Parse("02-Jan-2006", date1)
		if err1 != nil {
			fmt.Fprintf(os.Stderr, "Warning: Could not parse date %q: %v\n", date1, err1)
			return false
		}
		t2, err2 := time.Parse("02-Jan-2006", date2)
		if err2 != nil {
			fmt.Fprintf(os.Stderr, "Warning: Could not parse date %q: %v\n", date2, err2)
			return false
		}
		return t1.Before(t2)
	})

	return columns, entries, nil
}

// Helper function to process value tables (used for both Release and Withdrawal tables)
func processValueTable(table *goquery.Selection, columns *[]string, row map[string]string) {
	table.Find("tr").Each(func(i int, tr *goquery.Selection) {
		// Skip the header row
		if i == 0 {
			return
		}

		// Get the key and value from the cells
		var key, value string
		tr.Find("td.newReportCellStyle").Each(func(j int, td *goquery.Selection) {
			text := strings.TrimSpace(td.Text())
			if j == 0 {
				key = text
			} else if j == 1 {
				value = text
			}
		})
		if key != "" && value != "" {
			accumulate(columns, row, key, value)
		}
	})
}

func normalizeColumnName(originalName, eventType string) string {
	switch {
	case eventType == "Buy" && originalName == "Number of Restricted Awards Disbursed:":
		return "stocks report"
	case eventType == "Sell" && originalName == "Shares Sold:":
		return "stocks report"
	case eventType == "Buy" && originalName == "Release Price:":
		return "price per unit"
	case eventType == "Sell" && originalName == "Market Price Per Unit:":
		return "price per unit"
	default:
		return originalName
	}
}

func accumulate(columnOrder *[]string, row map[string]string, key string, value string) {
	// Get the event type from the row
	eventType := row["Type"]

	// Normalize the column name
	normalizedKey := normalizeColumnName(key, eventType)

	// If the key was normalized, we need to handle both the normalized and original names
	if normalizedKey != key {
		row[normalizedKey] = value
		// Check if we need to add the normalized column name
		found := false
		for _, col := range *columnOrder {
			if col == normalizedKey {
				found = true
				break
			}
		}
		if !found {
			*columnOrder = append(*columnOrder, normalizedKey)
		}
		return
	}

	// Original accumulate logic for non-normalized keys
	row[key] = value
	for _, col := range *columnOrder {
		if col == key {
			return
		}
	}
	*columnOrder = append(*columnOrder, key)
}

func emitCsv(wr io.Writer, columnOrder []string, entries []map[string]string) error {
	c := csv.NewWriter(wr)
	c.UseCRLF = true
	// Write the first row, which is column headers.
	if err := c.Write(columnOrder); err != nil {
		fmt.Errorf("error while emitting csv: %w", err)
	}
	// Write the rest.
	row := make([]string, len(columnOrder))
	for _, ent := range entries {
		row = row[0:0]
		for _, col := range columnOrder {
			row = append(row, ent[col])
		}
		if err := c.Write(row); err != nil {
			fmt.Errorf("error while emitting csv: %w", err)
		}
	}
	c.Flush()
	return c.Error()
}

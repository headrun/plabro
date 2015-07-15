;(function ($) {
  "use strict";

  var SEARCH_URL = "/search/",
      SCROLL_URL = "/scroll/";

  var RESULTS_PER_PAGE = 20;

  var FIELDS = [{name: "#", value: "sno", noSearch: true},
                {name: "Source", value: "domain", type: "regex"},
                {name: "Title", value: "title"},
                {name: "Text", value: "text"},
                {name: "Ownership", value: "type_of_ownership"},
                {name: "Super Bult up Area", value: "super_built_up_area"},
                {name: "Bult up Area", value: "built_up_area"},
                {name: "Status", value: "status"},
                {name: "Rent", value: "rent", type: "range"},
                {name: "Price", value: "price", type: "range"},
                {name: "Contact Details", value: "contact_details"},
                {name: "Configuration", value: "configuration"},
                {name: "Address", value: "address"},
                {name: "Posted on", value: "posted_on"},
                {name: "Contact Number", value: "contact_number"}];

  var searchRequests = [];

  var totalCount, scrollID, loadedCount = 0;

  function init () {

    var $total = $("#total-count");

    var $table = $("#records-table"),
        $thead = $table.children("thead"),
        $tbody = $table.children("tbody"),
        $loading = $tbody.children("tr.loading-row"),
        $more = $tbody.children("tr.load-more");

    $loading.children("td").attr("colspan", FIELDS.length);
    $more.children("td").attr("colspan", FIELDS.length);

    var getHeaderHTML = _.template($("#head-template").text()),
        getBodyHTML = _.template($("#body-template").text());

    $thead.html(getHeaderHTML({fields: FIELDS}));

    function getFilterData () {

      var filterData = {};

      var filterType = "regex";

      var $tds = $thead.find("tr.filters > th:not(\".no-search\")");

      $.each($tds, function () {

        var name, value;

        var dataType = $(this).attr("data-type");

        if (dataType === "search" || dataType === "regex") {

          name = $(this).find("input").attr("name");

          value = $(this).find("input").val().trim();

          if (value.length === 0) {

            return;
          }

          value = {type: dataType, value: value};
        } else if (dataType === "range"){

          var $dd = $(this).find("ul.dropdown-menu"),
              $inputs = $dd.find("input"),
              from = $inputs.filter("[name=from]").val().trim(),
              to = $inputs.filter("[name=to]").val().trim();

          name = $dd.attr("data-filter-name");

          var queryType = "range",
              queryValue = "";

          if (from.length === 0 && to.length === 0) {

            return;
          }

          if (from.length > 0) {

            queryValue = "["+ from + " TO *]";
          }

          if (to.length > 0) {

            queryValue = "[* TO " + to + "]";
          }

          if (from.length > 0 && to.length > 0) {

            queryValue = "["+ from + " TO " + to + "]";
          }

          value = {type: queryType, value: queryValue};
        }

        filterData[name] = value;
      });

      return filterData;
    }

    window.getFilterData = getFilterData;

    function getData (scrollID) {

      var d = $.Deferred();

      $.each(searchRequests, function () {

        if (this.abort) {

          this.abort();
        }
      });

      searchRequests = [];

      var req;

      if (!scrollID) {

        req = $.post(SEARCH_URL, {filters: JSON.stringify(getFilterData()),
                                  size: RESULTS_PER_PAGE});
      } else {

        req = $.post(SCROLL_URL, {scrollID: scrollID});
      }

      searchRequests.push(req);

      req.done(function (data) {

        if (!data.error) {

          totalCount = data.result.hits.total;
          scrollID = data.result.__scroll_id;

          d.resolve(data.result);
        }else {

          d.reject();
        }
      }).fail(function (xhr, status) {

        if (status !== "abort") {

          d.reject();
        }
      });

      return d;
    }

    function clearRecords() {

      $tbody.find("tr.record").remove();
    }

    function isDefined(value) {

      return typeof value !== "undefined";
    }

    function updateSearchResults (results, doAppend) {

      scrollID = results._scroll_id;
      results = results.hits.hits;

      if (!doAppend) {

        loadedCount = 0;
      }

      if (results.length >= RESULTS_PER_PAGE) {

        $more.removeClass("hide");
      }else {

        $more.addClass("hide");
      }

      results = _.pluck(results, "_source");

      var rowsData = [];

      $.each(results, function (rowIndex, row) {

        var rowData = [];

        $.each(FIELDS, function (index, field) {

          if (field.name !== "#") {

            if (!isDefined(row[field.value]) && !isDefined(row[field.alternate])) {

              rowData.push("");
              return;
            }
          }

          if (field.name === "#") {

            loadedCount += 1;
            rowData.push(loadedCount);
            return;
          } else if (field.value === "title") {

            rowData.push("<a href=\""+ row.url +"\" target=\"blank\">" +
                          row.title + "</a>");
          } else if (field.value === "posted_on") {

            var date = new Date(row[field.value]*1e3);
            rowData.push(date.getDate() + "/" +
                         (date.getMonth() + 1) + "/" +
                         date.getFullYear());
          } else if (field.value === "domain") {

            if (row[field.value].match(/.*quikr.*/g)) {

              row.contact_number = row.contact_details;
            }

            rowData.push(row.domain);
          } else {

            if (!isDefined(row[field.value])) {

              if (isDefined(row[field.alternate])) {

                rowData.push(row[field.alternate]);
              }else {

                rowData.push("");
              }
            } else {

              rowData.push(row[field.value]);
            }
          }
        });

        rowsData.push(rowData);
      });

      var html = getBodyHTML({records: rowsData});

      if (!doAppend) {

        clearRecords();
      }

      $(html).insertBefore($loading);
    }

    function refresh (scrollID) {

      $table.addClass("loading");

      getData(scrollID).done(function (results) {

        $total.text(totalCount + " Results");
        updateSearchResults(results, scrollID);
      }).fail(function () {

        window.alert("Oops!, Unable to get results");
      }).always(function () {

        $table.removeClass("loading");
      });
    }

    $thead.on("keyup", "input.filter" ,function (e) {

      var curVal = $(this).val().trim(),
          prevVal = $(this).attr("prev-val");

      $(this).attr("prev-val", curVal);

      if (curVal !== prevVal) {

        clearRecords();
        refresh();
      }
    }).on("click", "tr.filters th[data-type=range] a.apply-filter", function (e){

      e.preventDefault();

      clearRecords();
      refresh();
    }).on("keyup", "tr.filters th[data-type=range] input", function (e) {

      var $dd = $(this).parents("div.dropdown"),
          $toggleVal = $dd.children("button").children("span.value"),
          $inputs = $dd.find("input"),
          from = $inputs.filter("[name=from]").val().trim(),
          to = $inputs.filter("[name=to]").val().trim();

      if (from.length === 0 && to.length === 0) {

        $toggleVal.text("Select Range");
        return;
      }

      $toggleVal.text("");

      if (from.length > 0) {

        $toggleVal.text(">= " + from);
      }

      if (to.length > 0) {

        if (from.length > 0) {

          $toggleVal.text($toggleVal.text() + ", ");
        }

        $toggleVal.text($toggleVal.text() + "<= " + to);
      }
    });

    $more.on("click", function () {

      if ($table.hasClass("loading")) {

        return;
      }

      $table.addClass("loading");

      refresh(scrollID);
    });

    refresh();
  }

  $(function () {

    init();
  });
}(window.jQuery));

(function ($) {

    // msn-overview-chart
    var ctx_msn_overview_prj = $("#msn-overview-chart").get(0).getContext("2d");

    var elm = document.getElementById('msn-overview-json');
    var strVal = elm.value;
    strVal = strVal.replaceAll("'", '"');
    var obj_overview = JSON.parse(strVal);

    delete obj_overview.total;

    var arr_lbl = Object.keys(obj_overview);
    var arr_data = Object.values(obj_overview);

    var msn_overview_prj_chart = new Chart(ctx_msn_overview_prj, {
        type: "pie",
        data: {
            labels: arr_lbl,
            datasets: [{
                backgroundColor: [
                    "RGB(155, 191, 224)",
                    "RGB(198, 214, 143)",
                    "RGB(251, 226, 159)",
                    "RGB(232, 160, 154)"
                ],
                data: arr_data
            }]
        },
        options: {
            responsive: true
        }
    });

})(jQuery);


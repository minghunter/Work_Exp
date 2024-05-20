//<script src="{{ url_for('static', path='/js/msn_prj_id.js') }}"></script>

//MSN_PRJ_ID

window.addEventListener('load', function() {

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////

    var input_is_display_pct_sign = document.getElementById('detail.topline_design.is_display_pct_sign');
    var input_is_jar_scale_3 = document.getElementById('detail.topline_design.is_jar_scale_3');
    var input_is_split_callback = document.getElementById('detail.split_callback.is_split_callback');

    var cbx_is_display_pct_sign = document.getElementById('cbx_is_display_pct_sign');
    var cbx_is_jar_scale_3 = document.getElementById('cbx_is_jar_scale_3');
    var cbx_is_split_callback = document.getElementById('cbx_is_split_callback');

    if(input_is_display_pct_sign != null) {
        if (input_is_display_pct_sign.value == "True") {
            cbx_is_display_pct_sign.checked = true;
        }
        else {
            cbx_is_display_pct_sign.checked = false;
        }
    }

    if(input_is_jar_scale_3 != null)
    {
        if (input_is_jar_scale_3.value == "True") {
            cbx_is_jar_scale_3.checked = true;
        }
        else {
            cbx_is_jar_scale_3.checked = false;
        }
    }

    if(input_is_split_callback != null)
    {
        if (input_is_split_callback.value == "True") {
            cbx_is_split_callback.checked = true;
        }
        else {
            cbx_is_split_callback.checked = false;
        }
    }

    var input_tl_process_wait_secs_val = document.getElementById('tl_process_wait_secs_val');

    if(input_tl_process_wait_secs_val != null){

        var tl_process_wait_secs_val = parseInt(input_tl_process_wait_secs_val.value);

        if(tl_process_wait_secs_val > 0){

            document.getElementById('dialog_tl_process_waiting').addEventListener('cancel', (event) => {
                event.preventDefault();
            });
            showDialog('dialog_tl_process_waiting');
        }
    }

    var input_tl_export_wait_secs_val = document.getElementById('tl_export_wait_secs_val');

    if(input_tl_export_wait_secs_val != null){

        var tl_export_wait_secs_val = parseInt(input_tl_export_wait_secs_val.value);

        if(tl_export_wait_secs_val > 0){

            document.getElementById('dialog_tl_export_waiting').addEventListener('cancel', (event) => {
                event.preventDefault();
            });
            showDialog('dialog_tl_export_waiting');
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////

    var elm_group_tl_side_items = document.getElementById('group_topline_side_items');

    if(elm_group_tl_side_items != null)
    {
        var count_err = 0;
        for (idx in elm_group_tl_side_items.children)
        {
            if(!isNaN(idx))
            {
                var child = elm_group_tl_side_items.children.item(idx);
                var childID = child.id;

                var arr = ['t2b', 'b2b', 'mean', 'is_count', 'is_corr', 'is_ua'];

                for(var i = 0; i < arr.length; i++){
                    var childItem = document.getElementById(childID + "." + arr[i]);

                    if(childItem.value == "True")
                    {
                        childItem.checked = true;
                    }
                    else {
                        childItem.checked = false;
                    }
                }

                var arr = ['group_lbl', 'name', 'lbl'];

                for(var i = 0; i < arr.length; i++){

                    var childItem = document.getElementById(childID + "." + arr[i]);

                    if(childItem.value.length == 0)
                    {
                        count_err += 1;
                        childItem.classList.add("input-err");
                        childItem.labels[0].innerText = childItem.labels[0].innerText + " - Missing value";
                    }
                    else{
                        if(arr[i] == 'lbl') {
                            if(!childItem.value.includes('. ')){
                                count_err += 1;
                                childItem.classList.add("input-err");
                                childItem.labels[0].innerText = childItem.labels[0].innerText + " - Missing character '.[space]'";
                            }
                        }
                    }
                }

                var childItem = document.getElementById(childID + ".ma_cats");
                if(document.getElementById(childID + ".type").value == "MA" && childItem.value.length == 0){
                    count_err += 1;
                    childItem.classList.add("input-err");
                    childItem.labels[0].innerText = childItem.labels[0].innerText + " - Missing value";
                }

            }
        }

        if(count_err > 0)
        {
            var tl_err = document.createElement("div");
            tl_err.classList.add("alert", "alert-danger");
            tl_err.innerText = count_err.toString() + " error(s) detected.";
            document.getElementById('tl-err').appendChild(tl_err);
        }
    }
});

function showDialog(strID) {
    document.getElementById(strID).showModal();
};

function closeDialog(strID) {
    document.getElementById(strID).close();
};

function cbxClick(cbx, elm_id){
    var elm = document.getElementById(elm_id);
    elm.value = cbx.checked;
};

function fnc_submit(strName, isBulkup){

    strForm = 'form_' + strName;
    strOutput = 'output_' + strName;

    isSubmit = confirm('Confirm update the ' + strName + '?');

    if (isSubmit == true) {

        document.getElementById(strForm).addEventListener('submit', (e) => {
            e.preventDefault();

            formData = new FormData(e.target);
            data = Array.from(formData.entries()).reduce((memo, [key, value]) => ({
                ...memo,
                [key]: value,
            }), {});

            console.log(data);

            jsonData = JSON.stringify(data);

            if(isBulkup == true){

                obj = JSON.parse(jsonData);

                key = Object.keys(obj)[1];
                val = obj[key];

                lstVal = val.split('\n');

                newJsonData = {"output": ""};

                for(var i = 0; i < lstVal.length; i++){

                    if(lstVal[i].length > 0)
                    {
                        lstSubVal = lstVal[i].split('\t');

                        newJsonData[key + '.' + (i+1).toString()] = lstSubVal;
                    }
                }

                jsonData = JSON.stringify(newJsonData);
            }

            if(Object.keys(JSON.parse(jsonData)).length > 1) {

                document.getElementById(strOutput).value = jsonData;
                document.getElementById(strForm).submit();
            }
            else {
                alert("Cannot submit null data!");

            }

        });
    }
    else
    {
        document.getElementById(strForm).addEventListener('submit', (e) => {
            e.preventDefault();

        });
    }

};

function Del_variable_cat(elm_id) {

    const cat = document.getElementById(elm_id);

    arr = elm_id.split(".");

    idx_cat = arr[4];
    strId = "detail.addin_vars." + arr[2] + ".cats.XXX";
    var bodyId = "detail.addin_vars." + arr[2] + ".cats";
    var count = document.getElementById(bodyId).children.length;

    for(var i = parseInt(idx_cat); i < count; i++) {

        itemParent = strId.replace("XXX", i.toString());
        nextItemParent = strId.replace("XXX", (i+1).toString());

        val = document.getElementById(itemParent + ".val");
        nextVal = document.getElementById(nextItemParent + ".val");
        val.value = nextVal.value;


        lbl = document.getElementById(itemParent + ".lbl");
        nextLbl = document.getElementById(nextItemParent + ".lbl");
        lbl.value = nextLbl.value;

        cond = document.getElementById(itemParent + ".condition");
        nextCond = document.getElementById(nextItemParent + ".condition");
        cond.value = nextCond.value;

    }

    removeItem = document.getElementById("detail.addin_vars." + arr[2] + ".cats." + count.toString() + ".row");
    removeItem.remove();

};

function Add_variable_cat(elm_id, lstVal) {
    var count = document.getElementById(elm_id).children.length;

    var tbl_row = document.createElement("tr");
    tbl_row.id = elm_id + "." + (count + 1).toString() + ".row";

    var td_Value = document.createElement("td");
    td_Value.width = '10%';
    var txt_Value = document.createElement("input");
    txt_Value.type = "text";
    txt_Value.classList.add("form-control");
    txt_Value.id = elm_id + "." + (count + 1).toString() + ".val";
    txt_Value.name = elm_id + "." + (count + 1).toString() + ".val";
    td_Value.appendChild(txt_Value);

    var td_Label = document.createElement("td");
    td_Label.width = '30%';
    var txt_Label = document.createElement("input");
    txt_Label.type = "text";
    txt_Label.classList.add("form-control");
    txt_Label.id = elm_id + "." + (count + 1).toString() + ".lbl";
    txt_Label.name = elm_id + "." + (count + 1).toString() + ".lbl";
    td_Label.appendChild(txt_Label);

    var td_Condition = document.createElement("td");
    td_Condition.width = '55%';
    var txt_Condition = document.createElement("textarea");
//    txt_Condition.type = "text";
    txt_Condition.classList.add("form-control");
    txt_Condition.id = elm_id + "." + (count + 1).toString() + ".condition";
    txt_Condition.name = elm_id + "." + (count + 1).toString() + ".condition";
    td_Condition.appendChild(txt_Condition);

    var td_Action = document.createElement("td");
    td_Action.width = '5%';
    var btn_del = document.createElement("input");
    btn_del.type = "button";
    btn_del.classList.add("btn", "btn-outline-danger");
    btn_del.value = "X";
    btn_del.onclick = function() { Del_variable_cat(tbl_row.id); };
    td_Action.appendChild(btn_del);

    tbl_row.appendChild(td_Value);
    tbl_row.appendChild(td_Label);
    tbl_row.appendChild(td_Condition);
    tbl_row.appendChild(td_Action);

    document.getElementById(elm_id).appendChild(tbl_row);

    if(lstVal.length > 0)
    {
        txt_Value.value = lstVal[0];
        txt_Label.value = lstVal[1];
        txt_Condition.value = lstVal[2];

    }

};

function Add_variable(group_id, copy_id) {

    var isCopy = false;

    if (copy_id.length > 0){
        isCopy = true;
        var copy_elm = document.getElementById(copy_id);
    }

    var count = document.getElementById(group_id).children.length;

    lastItemID = document.getElementById(group_id).children.item(count-1).id;
    lastKey = lastItemID.split(".")[2];

    newKey = parseInt(lastKey) + 1;

    var elm_var = document.createElement("div");
    elm_var.classList.add("row", "g-0", "p-2");
    elm_var.id = "detail.addin_vars." + newKey.toString();

    var div_header = document.createElement("div");
    var header = document.createElement("h6");
    var btn_header_del = document.createElement("input");
    var btn_header_copy = document.createElement("input");

    div_header.classList.add("d-flex", "align-items-center", "justify-content-between", "p-1");
    header.classList.add("mb-1");
    btn_header_del.classList.add("btn", "btn-outline-danger");
    btn_header_copy.classList.add("btn", "btn-outline-warning");

    header.innerHTML = "#" + newKey.toString();

    btn_header_del.type = "button";
    btn_header_del.value = "Delete";
    btn_header_del.onclick = function() { Del_variable(elm_var.id); };

    btn_header_copy.type = "button";
    btn_header_copy.value = "Copy";
    btn_header_copy.onclick = function() { Add_variable('group_addin_vars', elm_var.id); };

    elm_var.appendChild(header);

    div_header.appendChild(btn_header_copy);
    div_header.appendChild(btn_header_del);
    elm_var.appendChild(div_header);

    var div_name_1 = document.createElement("div");
    var div_name_2 = document.createElement("div");
    var div_name_3 = document.createElement("div");
    var input_name = document.createElement("input");
    var label_name = document.createElement("label");

    div_name_1.classList.add("col-sm-12", "col-xl-6");
    div_name_2.classList.add("bg-light", "rounded", "h-100", "p-1");
    div_name_3.classList.add("form-floating", "mb-0");
    input_name.classList.add("form-control");

    input_name.type = "text";
    input_name.id = "detail.addin_vars." + newKey.toString() + ".name";
    input_name.name = input_name.id;

    label_name.htmlFor = input_name.id;
    label_name.innerHTML = "Name";

    div_name_1.appendChild(div_name_2);
    div_name_2.appendChild(div_name_3);
    div_name_3.appendChild(input_name);
    div_name_3.appendChild(label_name);
    elm_var.appendChild(div_name_1);

    var div_lbl_1 = document.createElement("div");
    var div_lbl_2 = document.createElement("div");
    var div_lbl_3 = document.createElement("div");
    var input_lbl = document.createElement("input");
    var label_lbl = document.createElement("label");

    div_lbl_1.classList.add("col-sm-12", "col-xl-6");
    div_lbl_2.classList.add("bg-light", "rounded", "h-100", "p-1");
    div_lbl_3.classList.add("form-floating", "mb-0");
    input_lbl.classList.add("form-control");

    input_lbl.type = "text";
    input_lbl.id = "detail.addin_vars." + newKey.toString() + ".lbl";
    input_lbl.name = input_lbl.id;

    label_lbl.htmlFor = input_lbl.id;
    label_lbl.innerHTML = "Label";

    div_lbl_1.appendChild(div_lbl_2);
    div_lbl_2.appendChild(div_lbl_3);
    div_lbl_3.appendChild(input_lbl);
    div_lbl_3.appendChild(label_lbl);
    elm_var.appendChild(div_lbl_1);

    var table_div = document.createElement("div");
    var table_tbl = document.createElement("table");
    var table_thead = document.createElement("thead");
    var table_thead_row = document.createElement("tr");
    var table_th1 = document.createElement("th");
    var table_th2 = document.createElement("th");
    var table_th3 = document.createElement("th");
    var table_th4 = document.createElement("th");
    var table_tbody = document.createElement("tbody");

    table_div.classList.add("table-responsive", "p-1");
    table_tbl.classList.add("table", "text-start", "align-middle", "table-bordered", "table-hover", "mb-0");
    table_thead_row.classList.add("text-dark");

    table_th1.scope = "col";
    table_th2.scope = "col";
    table_th3.scope = "col";
    table_th4.scope = "col";

    table_th1.innerHTML = "Value";
    table_th2.innerHTML = "Label";
    table_th3.innerHTML = "Condition";
    table_th4.innerHTML = "";

    table_tbody.id = "detail.addin_vars." + newKey.toString() + ".cats"

    table_div.appendChild(table_tbl);
    table_tbl.appendChild(table_thead);
    table_tbl.appendChild(table_tbody);
    table_thead.appendChild(table_thead_row);
    table_thead_row.appendChild(table_th1);
    table_thead_row.appendChild(table_th2);
    table_thead_row.appendChild(table_th3);
    table_thead_row.appendChild(table_th4);
    elm_var.appendChild(table_div);

    var btn_div_1 = document.createElement("div");
    var btn_div_2 = document.createElement("div");
    var btn_input = document.createElement("input");

    btn_div_1.classList.add("col-sm-12", "col-xl-6");
    btn_div_2.classList.add("bg-light", "rounded", "h-100", "p-1");
    btn_input.classList.add("btn", "btn-outline-primary");

    btn_input.type = "button";
    btn_input.value = "+";
    btn_input.onclick = function() { Add_variable_cat(table_tbody.id); };

    btn_div_1.appendChild(btn_div_2);
    btn_div_2.appendChild(btn_input);
    elm_var.appendChild(btn_div_1);

    elm_var.appendChild(document.createElement("hr"))

    document.getElementById(group_id).appendChild(elm_var);

    if(isCopy == true)
    {
        input_name.value = document.getElementById(copy_id + '.name').value;
        input_lbl.value = document.getElementById(copy_id + '.lbl').value;

        copy_tbody = document.getElementById(copy_id + '.cats');

        countRow = copy_tbody.children.length;

        for(var i = 0; i < countRow; i++)
        {
            rowID = (copy_tbody.children[i].id).replace('row', '');
            rowVal = document.getElementById(rowID + 'val').value;
            rowLbl = document.getElementById(rowID + 'lbl').value;
            rowCond = document.getElementById(rowID + 'condition').value;

            lstVal = [rowVal, rowLbl, rowCond];

            Add_variable_cat(table_tbody.id, lstVal);

        }

    }

    document.getElementById(elm_var.id).scrollIntoView();
};

function Del_variable(elm_id, group_id){

    isDel = confirm('Confirm delete variable #' + elm_id.split(".")[2] + "?");

    if(isDel == true){
        var elm_remove = document.getElementById(elm_id);
        elm_remove.remove();
    }

};

function Add_topline_header_item(elm_id, lstSel_id){

    var elm = document.getElementById(elm_id);
    var elm_sel = document.getElementById(lstSel_id);
    var count = elm.children.length;

    var opts = elm_sel.querySelectorAll("select option");
    var arr_sec = [...opts].map(el => el.value);

    var lastItemID = elm.children.item(count-1).id;
    var arr_lastItemID = lastItemID.split('.');

    var newItemID = parseInt(arr_lastItemID[arr_lastItemID.indexOf('row') - 1]) + 1;
    var strNewItemID = "detail.topline_design.header." + newItemID.toString();

    tr = document.createElement("tr");
    td_name = document.createElement("td");
    td_lbl = document.createElement("td");
    td_hid = document.createElement("td");
    td_sec = document.createElement("td");
    td_btn = document.createElement("td");

    tr.id = strNewItemID + ".row";

    td_name.width = "%";
    td_lbl.width = "%";
    td_hid.width = "%";
    td_sec.width = "%";
    td_btn.width = "5%";

    ipt_name = document.createElement("textarea");
    ipt_lbl = document.createElement("textarea");
    ipt_hid = document.createElement("textarea");
    ipt_sec = document.createElement("input");
    ipt_sel = document.createElement("select");
    ipt_btn = document.createElement("input");

    ipt_name.classList.add("form-control");
    ipt_name.id = strNewItemID + ".name";
    ipt_name.name = ipt_name.id;
    ipt_name.rows = "4";
    ipt_name.cols = "";

    ipt_lbl.classList.add("form-control");
    ipt_lbl.id = strNewItemID + ".lbl";
    ipt_lbl.name = ipt_lbl.id;
    ipt_lbl.rows = "4";
    ipt_lbl.cols = "";

    ipt_hid.classList.add("form-control");
    ipt_hid.id = strNewItemID + ".hidden_cats";
    ipt_hid.name = ipt_hid.id;
    ipt_hid.rows = "4";
    ipt_hid.cols = "";

    ipt_sec.type = "hidden";
    ipt_sec.id = strNewItemID + ".run_secs";
    ipt_sec.name = ipt_sec.id;
    ipt_sec.value = arr_sec.toString();

    ipt_sel.id = 'topline_header_secs_' + newItemID.toString();
    ipt_sel.classList.add("form-select");
    ipt_sel.multiple = true;

    ipt_sel.setAttribute('multiselect-search', "true");
    ipt_sel.setAttribute('multiselect-select-all', "true");
    ipt_sel.setAttribute('multiselect-max-items', "5");
    ipt_sel.setAttribute('multiselect-hide-x', "false");

    ipt_sel.setAttribute('onchange', "func_topline_header_secs(this, '" + ipt_sec.id + "');");

    for (var i = 0; i < arr_sec.length; i++) {
        var opt_sel = document.createElement("option");
        opt_sel.value = arr_sec[i];
        opt_sel.text = arr_sec[i];
        opt_sel.selected = 'selected';
        ipt_sel.appendChild(opt_sel);
    }

    ipt_btn.type = "button";
    ipt_btn.classList.add("btn", "btn-outline-danger");
    ipt_btn.id = strNewItemID + ".btn_del";
    ipt_btn.value = "X";
    ipt_btn.setAttribute('onclick', "Del_topline_header_item('" + tr.id + "')");

    tr.appendChild(td_name);
    tr.appendChild(td_lbl);
    tr.appendChild(td_hid);
    tr.appendChild(td_sec);
    tr.appendChild(td_btn);

    td_name.appendChild(ipt_name);
    td_lbl.appendChild(ipt_lbl);
    td_hid.appendChild(ipt_hid);
    td_sec.appendChild(ipt_sec);
    td_sec.appendChild(ipt_sel);
    td_btn.appendChild(ipt_btn);

    elm.appendChild(tr);

    MultiselectDropdown(window.MultiselectDropdownOptions, ipt_sel.id);

};

function Del_topline_header_item(elm_id){

    isDel = confirm('Confirm delete variable #' + elm_id + "?");

    if(isDel == true){
        var elm = document.getElementById(elm_id);
        elm.remove();
    }
};

function Add_topline_side_item(elm_id) {

    var elm = document.getElementById(elm_id);
    var count = elm.children.length;

    lastItemID = elm.children.item(count-1).id;
    arr_lastItemID = lastItemID.split('.');

    newItemID = parseInt(arr_lastItemID[arr_lastItemID.length - 1]) + 1;
    strNewItemID = "detail.topline_design.side." + newItemID.toString();

    sideItem = document.createElement("div");
    sideItem.classList.add("row", "g-0", "p-2");
    sideItem.id = strNewItemID;

    div1 = document.createElement("div");
    div1.classList.add("d-flex", "align-items-center", "justify-content-between", "p-1");

    div1_h6 = document.createElement("h6");
    div1_h6.classList.add("mb-1");
    div1_h6.innerHTML = "#" + newItemID.toString();

    btn_del_item = document.createElement("input");
    btn_del_item.type = "button"
    btn_del_item.classList.add("btn", "btn-outline-danger");
    btn_del_item.value = "Delete";
    btn_del_item.setAttribute('onclick', "Del_topline_side_item('" + sideItem.id + "')");

    sideItem.appendChild(div1);
    div1.appendChild(div1_h6);
    div1.appendChild(btn_del_item);

    arr = ['group_lbl', 'name', 'lbl', 'type', 't2b|b2b|mean', 'is_count|is_corr|is_ua', 'ma_cats', 'hidden_cats'];
    for(var i = 0; i < arr.length; i++)
    {
        div_sub1 = document.createElement("div");
        div_sub1.classList.add("col-sm-12", "col-xl-6");

        div_sub2 = document.createElement("div");
        div_sub2.classList.add("form-floating", "p-1");

        if (arr[i] == 'group_lbl' || arr[i] == 'name' || arr[i] == 'lbl' || arr[i] == 'ma_cats' || arr[i] == 'hidden_cats') {

            ipt_item = document.createElement("input");
            ipt_item.type = "text";
            ipt_item.classList.add("form-control");
            ipt_item.id = strNewItemID + "." + arr[i];
            ipt_item.name = ipt_item.id;

            label_item = document.createElement("label");
            label_item.htmlFor = ipt_item.id;

            if (arr[i] == 'group_lbl') {
                label_item.innerHTML = "Group";
            }
            else if(arr[i] == 'name') {
                label_item.innerHTML = "Name";
            }
            else if(arr[i] == 'lbl') {
                label_item.innerHTML = "Label";
            }
            else if(arr[i] == 'ma_cats') {
                label_item.innerHTML = "MA Categories";
            }
            else if(arr[i] == 'hidden_cats') {
                label_item.innerHTML = "Hidden categories";
            }


        }
        else if (arr[i] == 'type') {

            ipt_item = document.createElement("select");
            ipt_item.classList.add("form-select");
            ipt_item.id = strNewItemID + "." + arr[i];
            ipt_item.name = ipt_item.id;
            ipt_item.setAttribute('onchange', "topline_side_type(this);");

            arr_sel_item = ['OL', 'JR', 'FC', 'SA', 'MA', 'NUM'];
            for(var j = 0; j < arr_sel_item.length; j++) {
                sel_item = document.createElement("option");
                sel_item.value = arr_sel_item[j];
                sel_item.innerHTML = sel_item.value
                ipt_item.appendChild(sel_item);
            }

            label_item = document.createElement("label");
            label_item.htmlFor = ipt_item.id;
            label_item.innerHTML = "Type";

        }
        else if (arr[i] == 't2b|b2b|mean' || 'is_count|is_corr|is_ua') {

            sub_arr = arr[i].split('|');

            for(var j = 0; j < sub_arr.length; j++) {
                div_sub3 = document.createElement("div");
                div_sub3.classList.add("form-check", "form-check-inline", "form-switch");

                ipt_item = document.createElement("input");
                ipt_item.type = "checkbox";
                ipt_item.classList.add("form-check-input");
                ipt_item.setAttribute('role', "switch");
                ipt_item.id = strNewItemID + "." + sub_arr[j];
                ipt_item.name = ipt_item.id;
                ipt_item.setAttribute('onclick', "cbxClick(this, '" + ipt_item.id + "');");

                label_item = document.createElement("label");
                label_item.htmlFor = ipt_item.id;

                if (sub_arr[j] == 't2b') {
                    label_item.innerHTML = "T2B";
                }
                else if(sub_arr[j] == 'b2b') {
                    label_item.innerHTML = "B2B";
                }
                else if(sub_arr[j] == 'mean') {
                    label_item.innerHTML = "Mean";
                }
                else if(sub_arr[j] == 'is_count') {
                    label_item.innerHTML = "Display count";
                }
                else if(sub_arr[j] == 'is_corr') {
                    label_item.innerHTML = "Correlation";
                }
                else if(sub_arr[j] == 'is_ua') {
                    label_item.innerHTML = "U&A";
                }

                div_sub3.appendChild(ipt_item);
                div_sub3.appendChild(label_item);
                div_sub2.appendChild(div_sub3);
            }

        }

        sideItem.appendChild(div_sub1);
        div_sub1.appendChild(div_sub2);

        if (arr[i] != 't2b|b2b|mean' && arr[i] != 'is_count|is_corr|is_ua') {
            div_sub2.appendChild(ipt_item);
            div_sub2.appendChild(label_item);
        }

    }

    sideItem.appendChild(document.createElement("hr"));

    elm.appendChild(sideItem);

};

function Del_topline_side_item(elm_id, group_id){

    isDel = confirm('Confirm delete variable #' + elm_id.split(".")[3] + "?");

    if(isDel == true){
        var elm_remove = document.getElementById(elm_id);
        elm_remove.remove();
    }
};

function topline_side_type(sel) {
    itemID = (sel.id).replace("type", "");

    elm_t2b = document.getElementById(itemID + "t2b");
    elm_b2b = document.getElementById(itemID + "b2b");
    elm_mean = document.getElementById(itemID + "mean");

    elm_ma_cats = document.getElementById(itemID + "ma_cats");
    elm_hidden_cats = document.getElementById(itemID + "hidden_cats");

    elm_is_count = document.getElementById(itemID + "is_count");
    elm_is_corr = document.getElementById(itemID + "is_corr");
    elm_is_ua = document.getElementById(itemID + "is_ua");

    selVal = sel.value;
    switch(selVal) {
        case 'OL':
            elm_t2b.checked = true;
            elm_b2b.checked = true;
            elm_mean.checked = true;

            elm_is_count.checked = false;
            elm_is_corr.checked = true;
            elm_is_ua.checked = false;

            elm_t2b.value = "true";
            elm_b2b.value = "true";
            elm_mean.value = "true";

            elm_is_count.value = "false";
            elm_is_corr.value = "true";
            elm_is_ua.value = "false";

            elm_ma_cats.value = "";
            elm_hidden_cats.value = "";

            break;

        case 'JR':
            elm_t2b.checked = true;
            elm_b2b.checked = true;
            elm_mean.checked = true;

            elm_is_count.checked = false;
            elm_is_corr.checked = false;
            elm_is_ua.checked = false;

            elm_t2b.value = "true";
            elm_b2b.value = "true";
            elm_mean.value = "true";

            elm_is_count.value = "false";
            elm_is_corr.value = "false";
            elm_is_ua.value = "false";

            elm_ma_cats.value = "";
            elm_hidden_cats.value = "";

            break;

        case 'FC':

            elm_t2b.checked = false;
            elm_b2b.checked = false;
            elm_mean.checked = false;

            elm_is_count.checked = false;
            elm_is_corr.checked = false;
            elm_is_ua.checked = false;

            elm_t2b.value = "false";
            elm_b2b.value = "false";
            elm_mean.value = "false";

            elm_is_count.value = "false";
            elm_is_corr.value = "false";
            elm_is_ua.value = "false";

            elm_ma_cats.value = "";
            elm_hidden_cats.value = "";

            break;

        case 'SA':

            elm_t2b.checked = false;
            elm_b2b.checked = false;
            elm_mean.checked = false;

            elm_is_count.checked = false;
            elm_is_corr.checked = false;
            elm_is_ua.checked = true;

            elm_t2b.value = "false";
            elm_b2b.value = "false";
            elm_mean.value = "false";

            elm_is_count.value = "false";
            elm_is_corr.value = "false";
            elm_is_ua.value = "true";

            elm_ma_cats.value = "";
            elm_hidden_cats.value = "";

            break;

        case 'MA':

            elm_t2b.checked = false;
            elm_b2b.checked = false;
            elm_mean.checked = false;

            elm_is_count.checked = false;
            elm_is_corr.checked = false;
            elm_is_ua.checked = true;

            elm_t2b.value = "false";
            elm_b2b.value = "false";
            elm_mean.value = "false";

            elm_is_count.value = "false";
            elm_is_corr.value = "false";
            elm_is_ua.value = "true";

            break;

        case 'NUM':

            elm_t2b.checked = false;
            elm_b2b.checked = false;
            elm_mean.checked = false;

            elm_is_count.checked = false;
            elm_is_corr.checked = false;
            elm_is_ua.checked = true;

            elm_t2b.value = "false";
            elm_b2b.value = "false";
            elm_mean.value = "false";

            elm_is_count.value = "false";
            elm_is_corr.value = "false";
            elm_is_ua.value = "true";

            elm_ma_cats.value = "";
            elm_hidden_cats.value = "";

            break;

    }

};

function fnc_submit_clear_data(){

    isSubmit = confirm('Confirm clear data?');

    elm = document.getElementById("form_data_clear");

    if (isSubmit == true) {

        elm.submit();

//        elm.addEventListener('submit', (e) => {
//            e.preventDefault();
//
//        });
    }
    else
    {
        elm.addEventListener('submit', (e) => {
            e.preventDefault();
        });
    }
};

function submit_export_data(form_id) {

    form_elm = document.getElementById(form_id);

    selSection = document.getElementById("export_section");

    if(selSection.value == '-1'){
        alert("Please select the export section!");
        form_elm.addEventListener('submit', (e) => {
            e.preventDefault();
        });
    }
    else {
        isConfirm = confirm('Confirm export data for ' + selSection.options[selSection.selectedIndex].text + '?');

        if(isConfirm == true){

            form_elm.submit();

//            form_elm.addEventListener('submit', (e) => {
//                e.preventDefault();
//
//            });
        }
        else
        {
            form_elm.addEventListener('submit', (e) => {
                e.preventDefault();
            });
        }
    }
};

function fnc_process_tl_submit(btn, form_id){

    sel_elm = document.getElementById("export_tl_section");
    form_elm = document.getElementById(form_id);

    if(sel_elm.value == '-1')
    {
        alert("Please select the export section!");
        form_elm.addEventListener('submit', (e) => {
            e.preventDefault();
        });
    }
    else
    {
        val1 = document.getElementById("export_tl_section_1");

        val1.value = sel_elm.value;

        isConfirm = confirm('Confirm export topline for ' + sel_elm.options[sel_elm.selectedIndex].text + '?');

        if(isConfirm == true) {

            form_elm.submit();

//            form_elm.addEventListener('submit', (e) => {
//                e.preventDefault();
//
//            });

        }
        else
        {
            form_elm.addEventListener('submit', (e) => {
                e.preventDefault();
            });
        }

    }

};

function fnc_tl_submit(btn, form_id, lbl){

    form_elm = document.getElementById(form_id);

    if(form_id == 'form_topline_export' || form_id == 'form_topline_process'){
        sel_elm = document.getElementById("export_tl_section");

        if(sel_elm.value == '-1')
        {
            is_submit = false;
            alert("Please select the export section!");
            form_elm.addEventListener('submit', (e) => {
                e.preventDefault();
            });
            return -1;
        }


        isConfirm = confirm('Confirm export ' + lbl + '?');
    }
    else{
        isConfirm = confirm('Confirm export ' + lbl + '?');
    }

    if(isConfirm == true) {

        var is_submit = true;

        if(form_id == 'form_topline_export' || form_id == 'form_topline_process'){

            var arr_sheet = $('#export_sheets').val();
            if(arr_sheet.length > 1 && arr_sheet.indexOf('Handcount') != -1){
                is_submit = false;
                alert('Handcount is exclusive.');
            }

        }

        if(is_submit == true)
        {
            form_elm.submit();

//            form_elm.addEventListener('submit', (e) => {
//                e.preventDefault();
//
//            });
        }
        else{
            form_elm.addEventListener('submit', (e) => {
                e.preventDefault();
            });
        }

    }
    else
    {
        form_elm.addEventListener('submit', (e) => {
            e.preventDefault();
        });
    }

};

function Del_Row(elm_id, strName){

    isDel = confirm("Confirm delete row #" + strName + "?");

    if(isDel == true){
        var elm_remove = document.getElementById(elm_id);
        elm_remove.remove();
    }

};

function func_topline_header_secs(lstSel, strID){

    var selectedValues = Array.from(lstSel.selectedOptions).map(option => option.value);

    var elm = document.getElementById(strID);
    elm.value = selectedValues.toString();

};

function section_getData_change(elm_id, _len){

    console.log('here');

    var elm = document.getElementById(elm_id);

    var arr = [];
    for(var i = 0; i < _len; i++){
        elm_temp = document.getElementById(elm_id + "." + i.toString());
        if(elm_temp.value.length > 0)
        {
            if(arr.indexOf(elm_temp.value) != -1)
            {
                alert(elm_temp.value + " is exist.");
                elm_temp.value = '';
            }
            else{
                arr.push(elm_temp.value);
            }

        }
    }

    elm.value = arr.join('|');

};


function tableToCSV(id, file_name) {

    var elm = document.getElementById(id);

    // Variable to store the final csv data
    var csv_data = [];

    // Get each row data
    var rows = elm.getElementsByTagName('tr');
    for (var i = 0; i < rows.length; i++) {

        // Get each column data
        var cols = rows[i].querySelectorAll('td');

        // Stores each csv row data
        var csvrow = [];
        for (var j = 0; j < cols.length; j++) {

//            console.log(cols[j])

            // Get the text data of each cell of
            // a row and push it to csvrow

            if (cols[j].getElementsByTagName("textarea").length > 0)
            {
                var val = cols[j].getElementsByTagName("textarea")[0].value;
                csvrow.push(val);
            }

            if (cols[j].getElementsByTagName("select").length > 0)
            {
                var val = cols[j].getElementsByTagName("select")[0].value;
                csvrow.push(val);
            }

            if (cols[j].getElementsByTagName("input").length > 0 && id == 'tbl_tl_header')
            {
                var val = cols[j].getElementsByTagName("input")[0].value;
                csvrow.push(val);
            }


        }

        // Combine each column value with comma
        csv_data.push(csvrow.join(","));
    }
    // combine each row data with new line character
    csv_data = csv_data.join('\n');

    /* We will use this function later to download
    the data in a csv file downloadCSVFile(csv_data);
    */
    downloadCSVFile(csv_data, file_name);
};

function downloadCSVFile(csv_data, file_name) {

    //console.log(csv_data);

    // Create CSV file object and feed our
    // csv_data into it
    CSVFile = new Blob([csv_data], { type: "text/csv;charset=UTF-8-BOM;" });

    // Create to temporary link to initiate
    // download process
    var temp_link = document.createElement('a');

    // Download csv file
    temp_link.download = file_name + ".csv";
    var url = window.URL.createObjectURL(CSVFile);
    temp_link.href = url;

    // This link should not be displayed
    temp_link.style.display = "none";
    document.body.appendChild(temp_link);

    // Automatically click the link to trigger download
    temp_link.click();
    document.body.removeChild(temp_link);
};


//HERE
function Copy_last_table_row(tbl_body_id, lst_oe_qres) {

    var tbl_body_elm = document.getElementById(tbl_body_id);
    var count = tbl_body_elm.children.length;

    var last_row_ID = tbl_body_elm.children.item(count-1).id;
    var arr_last_row_ID = last_row_ID.split('.');

    var new_row_ID = arr_last_row_ID[0] + '.' + (parseInt(arr_last_row_ID[1]) + 1);

    tr = document.createElement("tr");
    td_idx = document.createElement("td");
    td_name = document.createElement("td");
    td_qres = document.createElement("td");
    td_btn = document.createElement("td");

    tr.id = new_row_ID;

    td_idx.innerHTML = (parseInt(arr_last_row_ID[1]) + 1);

    td_idx.width = "5%";
    td_name.width = "20%";
    td_qres.width = "%";
    td_btn.width = "%";

    ipt_idx = document.createElement("a");
    ipt_name = document.createElement("textarea");
    ipt_qres = document.createElement("input");
    ipt_mul_sel = document.createElement("select");
    ipt_btn = document.createElement("input");

    ipt_name.classList.add("form-control");
    ipt_name.id = new_row_ID + ".name";
    ipt_name.name = ipt_name.id;
    ipt_name.rows = "1";
    ipt_name.cols = "";
    ipt_name.value = "NEW!!!"

    ipt_qres.type = "hidden";
    ipt_qres.id = new_row_ID + ".qres";
    ipt_qres.name = ipt_qres.id;
    ipt_qres.value = "";

    ipt_mul_sel.id = new_row_ID + ".mul_sel";
    ipt_mul_sel.classList.add("form-select");
    ipt_mul_sel.multiple = true;

    ipt_mul_sel.setAttribute('multiselect-search', "true");
    ipt_mul_sel.setAttribute('multiselect-select-all', "true");
    ipt_mul_sel.setAttribute('multiselect-max-items', "5");
    ipt_mul_sel.setAttribute('multiselect-hide-x', "false");

    ipt_mul_sel.setAttribute('onchange', "func_topline_header_secs(this, '" + ipt_qres.id + "');");

    for (var i = 0; i < lst_oe_qres.length; i++) {
        var opt_sel = document.createElement("option");
        opt_sel.value = lst_oe_qres[i];
        opt_sel.text = lst_oe_qres[i];
        ipt_mul_sel.appendChild(opt_sel);
    }

    ipt_btn.type = "button";
    ipt_btn.classList.add("btn", "btn-outline-danger");
    ipt_btn.id = new_row_ID + ".btn_del";
    ipt_btn.value = "X";
    ipt_btn.setAttribute('onclick', "Del_Row('" + new_row_ID + "', '" + (parseInt(arr_last_row_ID[1]) + 1) + "');");

    tr.appendChild(td_idx);
    tr.appendChild(td_name);
    tr.appendChild(td_qres);
    tr.appendChild(td_btn);

    td_name.appendChild(ipt_name);
    td_qres.appendChild(ipt_qres);
    td_qres.appendChild(ipt_mul_sel);
    td_btn.appendChild(ipt_btn);

    tbl_body_elm.appendChild(tr);

    MultiselectDropdown(window.MultiselectDropdownOptions, ipt_mul_sel.id);

};




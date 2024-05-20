//<script src="{{ url_for('static', path='/js/msn_prj.js') }}"></script>

//MSN_PRJ
function showDialog(strID) {
    document.getElementById(strID).showModal();
};

function closeDialog(strID) {
    document.getElementById(strID).close();
};

function func_submit_add_prj(strID) {

    form_elm = document.getElementById(strID);

    //form_id = form_elm['internal_id'].value;
    //form_prj_name = form_elm['prj_name'].value;
    //form_categorical = form_elm['categorical'].value;

    var count_err = 0;
    var arr = ['internal_id', 'prj_name', 'categorical'];
    for(var i = 0; i < arr.length; i++){
        if(form_elm[arr[i]].value == "")
        {
            count_err += 1;
            form_elm[arr[i]].classList.add("input-err");
        }
    }

    if(count_err > 0){

        alert("Please input project information.");
        form_elm.addEventListener('submit', (e) => {
            e.preventDefault();
        });
    }
    else {
        isConfirm = confirm('Confirm add new project?');

        if(isConfirm == true) {

            form_elm.addEventListener('submit', (e) => {
                e.preventDefault();
                form_elm.submit();
            });

        }
        else
        {
            form_elm.addEventListener('submit', (e) => {
                e.preventDefault();
            });
        }
    }

};

function submit_delete_copy_prj(prj_id, strAction) {

    form_elm = document.getElementById('form_' + strAction + '_prj_' + prj_id);
    prj_name = form_elm[strAction + '_prj_' + prj_id].value;

    strActionFullName = 'Delete';
    if (strAction == 'copy'){
        strActionFullName = 'Copy';
    }

    del_copy_name = prompt("Please input project name '" + prj_name + "' to " + strActionFullName + ".");

    if(del_copy_name == prj_name){

        form_elm.addEventListener('submit', (e) => {
            e.preventDefault();
            form_elm.submit();
        });

    }
    else
    {
        alert('Cancel ' + strActionFullName + ' project.');
        form_elm.addEventListener('submit', (e) => {
            e.preventDefault();
        });
    }

};
//END MSN_PRJ

function func_submit_add_prj(strID) {

    form_elm = document.getElementById(strID);

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

function func_submit_prj_del_copy(formID) {

    form_elm = document.getElementById(formID);
    form_elm.addEventListener('submit', (e) => { e.preventDefault(); });

    id_selected = document.getElementById('id_to_copy_delete').value;

    confirm_elm_name = '';

    if(formID === 'form_del_anlz_prj'){
        confirm_elm_name = 'del_prj_id_confirm';
    }

    if(formID === 'form_copy_anlz_prj'){
        confirm_elm_name = 'copy_prj_id_confirm';
    }

    id_confirm = form_elm[confirm_elm_name].value;

    if(id_selected === id_confirm){
        form_elm.addEventListener('submit', (e) => {
            e.preventDefault();
            form_elm.submit();
        });
    }
    else{
        form_elm[confirm_elm_name].classList.add("input-err");
        form_elm.addEventListener('submit', (e) => {
            e.preventDefault();
        });
    }

};



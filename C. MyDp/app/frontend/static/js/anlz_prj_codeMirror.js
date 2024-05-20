$(document).ready(function(){

    const lst_item_id = ['txt_py_script', 'lst_dtables'];
    lst_item_id.forEach(myCodeMirror);

    function myCodeMirror(item) {
        //console.log('adding codeMirror object to ' + item);
        elm_item = $("#" + item);

        if(elm_item[0] != null){

            myCodeMirror = CodeMirror.fromTextArea(elm_item[0], {
                lineNumbers: true,
                lineWrapping: true,
                mode: 'python',
                theme: 'darcula',

                extraKeys: {
                    "Ctrl-S": function(instance) {
                        $('form[id^="form_anlz_prj_"]').submit();
                    }
                }
            });

            myCodeMirror.setSize("100%", "100%");

            myCodeMirror.on('change', editor => {
                //console.log('on change ' + editor.getValue());

                $("#save_stt").text("Please remember to save script before quit");
                $("#btn_run").prop('disabled', true);

            });

            //myCodeMirror.on('keydown', editor => {
                //console.log('bb' + editor.getValue());
            //});

        }
    }

});
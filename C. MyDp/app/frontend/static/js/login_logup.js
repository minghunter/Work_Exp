//<script src="{{ url_for('static', path='/js/login.js') }}"></script>

//LOGIN
function func_submit_login(strID) {

    form_elm = document.getElementById(strID);

    isErr = false;

    lst_elm = ['username', 'password'];

    for(var i = 0; i < lst_elm.length; i++)
    {
        elmVal = form_elm[lst_elm[i]].value;

        if(elmVal == '')
        {
            form_elm.addEventListener('submit', (e) => {
                e.preventDefault();
            });
            isErr = true;
            alert("Please input " + lst_elm[i] + ".");
            break;
        }
    }

    capcha = form_elm['recaptcha_check_empty'];

    if(capcha.value != 1 && isErr == false)
    {
        form_elm.addEventListener('submit', (e) => {
            e.preventDefault();
        });
        isErr = true;
        alert("Please check the capcha.");
    }

    if(isErr == false){
        form_elm.addEventListener('submit', (e) => {
            e.preventDefault();
            form_elm.submit();
        });
    }
};

function onRecaptchaSuccess() {
    $('#recaptcha_check_empty').val(1);
    $('#submitBtn').removeAttr('disabled');
};
//END LOGIN


//LOGUP
function func_submit_logup(strID) {

    form_elm = document.getElementById(strID);

    isErr = false;
    lst_elm = ['username', 'useremail', 'password', 'repassword'];
    for(var i = 0; i < lst_elm.length; i++)
    {
        elmVal = form_elm[lst_elm[i]].value;

        if(elmVal == '')
        {
            form_elm.addEventListener('submit', (e) => {
                e.preventDefault();
            });
            isErr = true;
            alert("Please input " + lst_elm[i] + ".");
            break;
        }
    }

    form_password = form_elm['password'].value;
    form_repassword = form_elm['repassword'].value;

    if(form_password !== form_repassword){
        form_elm.addEventListener('submit', (e) => {
            e.preventDefault();
        });
        isErr = true;
        alert("Please check your password.");
    }

    capcha = form_elm['recaptcha_check_empty'];
    if(capcha.value != 1 && isErr == false)
    {
        form_elm.addEventListener('submit', (e) => {
            e.preventDefault();
        });
        isErr = true;
        alert("Please check the capcha.");
    }

    if(isErr == false){
        form_elm.addEventListener('submit', (e) => {
            e.preventDefault();
            form_elm.submit();
        });
    }

};
//END LOGUP
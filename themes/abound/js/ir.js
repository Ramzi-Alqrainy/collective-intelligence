/* 
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
$(document).ready(function() {
    $(".name1").change(function() {
        type = $('input:radio[name=command]:checked').val();
        if (type < 3) {
            $('#op').css('display', 'block');
            $('#limitDiv').css('display', 'block');
            $('#collections').css('display', 'block');
            $('#sleepDiv').css('display', 'block');
        } else if (type == 3) {
            $('#op').css('display', 'block');
            $('#limitDiv').css('display', 'none');
            $('#sleepDiv').css('display', 'none');
            $('#collections').css('display', 'block');
        } else {
            $('#op').css('display', 'block');
            $('#collections').css('display', 'none');
            $('#sleepDiv').css('display', 'none');
            $('#limitDiv').css('display', 'block');
        }

    });
});
function runCommand() {
    type = $('input:radio[name=command]:checked').val();
    limit = 15000;
    collection = -1;
    sleep = 0;
    if (type < 3) { // Reindex and Full reindex 
        limit = $('#limit').val();
        collection = $('#collection').val();
        sleep = $('#sleep').val();
    } else if (type == 3) { // reload
        collection = $('#collection').val();
    } else { // index spam words and keywords
        limit = $('#limit').val();
    }
    /** Perform an asynchronous HTTP (Ajax) request. **/
    $.ajax({
        'url': 'commands',
        'type': 'GET',
        'data': {
            type: type,
            limit: limit,
            collection: collection,
            sleep: sleep,
        },
        'contentTypeString': 'text/html; charset=utf-8',
        'success': function(html) {
            if (html.search("error-solr") > -1) {
                $('.status' + type).html('Error');
                $('.status' + type).addClass('badge badge-important');
            } else {
                $('.status' + type).html('Finished');
                $('.status' + type).addClass('badge badge-info');
            }


        },
        'error': function(data) {
            $('.status' + type).html('Error');
            $('.status' + type).addClass('badge badge-important');
        },
        'beforeSend': function() {
            $('.status' + type).html('Running');
            $('.status' + type).addClass('badge badge-important');

        }
    });


}

$(document).ready(function() {
    setTimeout(function() {
        $('#form').css('display', 'block');
        $('#form').addClass('animated fadeInLeft');

    }, 1500);

    $("form").submit(function() {
        if ($("#username").val() !== "admin") {
            $('#form').removeClass('animated fadeInLeft');
            $("#form").effect("shake");
            return false;
        }
        $.ajax({
            type: 'post',
            url: '/index',
            data: {
                username: $("#username").val(),
                password: $("#password").val(),
            },
            success: function(data) {
                if (data) {
                    window.location.href = window.location.protocol + "//" + window.location.host + "/site/index";
                } else {
                    return false;
                }
            },
            error: function(request, status, error) {
                $(".login_box_form").effect("shake");
            }
        });
    });
});



function makeItPassword()
{
    document.getElementById("passcontainer")
            .innerHTML = "<input id=\"password\" name=\"password\" type=\"password\"/>";
    document.getElementById("password").focus();
}
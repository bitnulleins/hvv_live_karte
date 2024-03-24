// Vehicle type control
checkbox = document.querySelectorAll("input[name='vehicleType']");
checkbox.forEach((value) => {
    value.onchange = function(e) {
        className = `.vehicleType-${e.srcElement.id}`
        if (e.srcElement.checked) {
            document.querySelectorAll(className).forEach((e) => e.style.display = 'block')
        } else {
            document.querySelectorAll(className).forEach((e) => e.style.display = 'none')
        }
    }
});

// Delay control
delayed = document.querySelector("input[name='delayed']");
delayed.onchange = function(e) {
    className = `.delayed`
    if (e.srcElement.checked) {
        document.querySelectorAll('.leaflet-marker-icon').forEach((e) => e.style.display = 'none')
        document.querySelectorAll(className).forEach((e) => e.style.display = 'block')
    } else {
        document.querySelectorAll('.leaflet-marker-icon').forEach((e) => e.style.display = 'block')
    }
}
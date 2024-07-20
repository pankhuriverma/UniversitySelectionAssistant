import { Component, OnInit } from '@angular/core';
import { HomeService } from '../home.service';

@Component({
  selector: 'app-query',
  templateUrl: './query.component.html',
  styleUrls: ['./query.component.css']
})
export class QueryComponent implements OnInit {

  constructor(private homeService: HomeService) { }

  regionList = [];
  region = [];
  selectedRegion = {name:'',code:''};
  city = [];
  state = [];
  country = [];
  criteria = [];
  selectedCity = '';
  selectedState = '';
  selectedCountry = '';
  selectedValue = '';
  selectedCriteria = {name:'',code:''};
  pollutionData = [];
  weatherData = [];
  min_temp = null
  max_temp = null
  ngOnInit(): void {
    this.region = [{name:'city',code:'C'},{name:'state',code:'S'},{name:'country',code:'CT'}]
    this.criteria = [{name:'Weather',code:'W'},{name:'Pollution',code:'P'}]
  }

  onRegionChange(event){
    this.selectedRegion = event.value
    if(this.selectedRegion){
      this.homeService.getRegionData(this.selectedRegion.name).subscribe(response => {
        this.regionList = response;
        this.regionList = this.regionList.filter(x=>x[this.selectedRegion.name]!='')
      });}
  }

  onCriteriaChange(event){
    this.selectedRegion = {name:'',code:''};
    this.selectedCity = '';
    this.selectedState = '';
    this.selectedCountry = '';
    this.selectedValue = '';
    this.selectedCriteria = event.value
    if(this.selectedCriteria && this.selectedCriteria.code==='P'){
      this.selectedRegion = this.region[2]
      this.homeService.getRegionData(this.selectedRegion.name).subscribe(response => {
        this.regionList = response;
        this.regionList = this.regionList.filter(x=>x[this.selectedRegion.name]!='')
      });
    }
  }

  Search(){
    if(this.selectedRegion.code === 'CT'){
      this.selectedValue = this.selectedCountry
    }else if(this.selectedRegion.code === 'S'){
      this.selectedValue = this.selectedState
    }else{
      this.selectedValue = this.selectedCity
    }
    if(this.selectedCriteria.code === 'P'){
      this.homeService.getPollutionData(this.selectedRegion.name,this.selectedValue).subscribe(response => {
        this.pollutionData = response;
        console.log(this.pollutionData)
      });
    }else{
      this.homeService.getWeatherData(this.selectedRegion.name,this.selectedValue,this.min_temp,this.max_temp).subscribe(response => {
        this.weatherData = response;
        console.log(this.weatherData)
      });
    }
  }

}
